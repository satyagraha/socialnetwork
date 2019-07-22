package com.goyeau.socialnetwork

import com.goyeau.kafka.streams.circe.CirceSerdes._
import com.goyeau.socialnetwork.model._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import io.circe.generic.auto._
import io.circe.java8.time._
import io.circe.syntax._
import java.util.Properties

import monocle.macros.syntax.lens._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.state.KeyValueStore

class DataProcessing(streamsBuilder: StreamsBuilder) {

  val usersStream = streamsBuilder.streamFromRecord[User]()
  val postsStream = streamsBuilder.streamFromRecord[Post]()
  val commentsStream = streamsBuilder.streamFromRecord[Comment]()
  val likesStream = streamsBuilder.streamFromRecord[Like]()

  private[this] implicit def materializer[K, V](implicit serdeK: Serde[K], serdeV: Serde[V]) =
    Materialized.`with`[K, V, KeyValueStore[Bytes, Array[Byte]]](serdeK, serdeV)

  val usersByKey = usersStream
    .groupByKey
    .reduce((first, second) => if (first.updatedOn.isAfter(second.updatedOn)) first else second)

  val postsByAuthor = postsStream
    .groupBy((_, post) => post.author)
    .aggregate(Map.empty[Id[Post], Post])((_, post, posts) =>
      if (posts.get(post.id).exists(_.updatedOn.isAfter(post.updatedOn))) posts
      else posts + (post.id -> post))
    .mapValues(_.values.toSet)

  val likesByKey = likesStream
    .groupByKey
    .aggregate(Set.empty[Like])((_, like, likes) => if (like.unliked) likes - like else likes + like)

  val commentCountByKey = commentsStream
    .groupByKey
    .aggregate(Set.empty[Id[Comment]])((_, comment, commentIds) =>
      if (comment.deleted) commentIds - comment.id else commentIds + comment.id)
    .mapValues(_.size)

  val denormalizedPostsByKey = postsByAuthor
    .join(usersByKey)((posts, author) =>
      posts.map(DenormalisedPost(_, author, DenormalisedPost.Interactions(Set.empty, 0))))
    .toStream
    .flatMapValues(_.toIterable)
    .groupBy((_, denormalisedPost) => denormalisedPost.post.id)
    .reduce((first, second) => if (first.post.updatedOn.isAfter(second.post.updatedOn)) first else second)
    .leftJoin(likesByKey)((denormalisedPost, likes) =>
      Option(likes).fold(denormalisedPost)(denormalisedPost.lens(_.interactions.likes).set(_)))
    .leftJoin(commentCountByKey)((denormalisedPost, commentCount) =>
      denormalisedPost.lens(_.interactions.comments).set(commentCount))
    .toStream

  def produceDenormalizedPosts(): Unit =
    denormalizedPostsByKey.toTopic
}

object DataProcessingApp extends App {
  lazy val configForApp = {
    val config = new Properties()
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Config.BootstrapServers)
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, getClass.getCanonicalName)
    config
  }

  println("Starting streams")
  val streamsBuilder = new StreamsBuilder()
  val dataProcessing = new DataProcessing(streamsBuilder)
  dataProcessing.produceDenormalizedPosts()
  val topology = streamsBuilder.build()
  val streams = new KafkaStreams(topology, configForApp)
  Runtime.getRuntime.addShutdownHook(new Thread(() => streams.close()))
  streams.start()
}
