package com.goyeau.socialnetwork

import java.net.URI
import java.time.Instant
import java.util.Properties

import com.goyeau.kafka.streams.circe.CirceSerdes._
import com.goyeau.socialnetwork.model._
import io.circe.generic.auto._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp
import org.apache.kafka.streams.{StreamsConfig, TopologyTestDriver}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.test.ConsumerRecordFactory
import org.scalatest.FlatSpec

object TestDataProcessing {

  class ConRecFactory[K, V](implicit record: Record[K, V], serdeK: Serde[K], serdeV: Serde[V]) {
    private[this] val crf = new ConsumerRecordFactory[K, V](record.topic, serdeK.serializer, serdeV.serializer, 0, 0)

    def create(v: V): ConsumerRecord[Array[Byte], Array[Byte]] =
      crf.create(record.topic, record.key(v), v, record.timestamp(v))
  }

  class ProdRecFactory[K, V](driver: TopologyTestDriver)(implicit record: Record[K, V], serdeK: Serde[K], serdeV: Serde[V]) {
    def readOutput(): Option[ProducerRecord[K, V]] =
      Option(driver.readOutput(record.topic, serdeK.deserializer, serdeV.deserializer))
  }
}

class TestDataProcessing extends FlatSpec {

  import TestDataProcessing._

  val configForTest = {
    val config = new Properties()
    config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, getClass.getCanonicalName)
    config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")
    config.setProperty(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, classOf[FailOnInvalidTimestamp].getName)
    config
  }

  "the system" should "round-trip users" ignore {
    val user = User(Id("user1"), Instant.now().minusSeconds(100000), new URI("http://user1"), "nick1", false, false)
    val serdeV: Serde[User] = implicitly
    val topic = "some-topic"
    val bytes = serdeV.serializer.serialize(topic, user)
    Thread.sleep(1000)
    val result = serdeV.deserializer.deserialize(topic, bytes)
    assert(result == user)
  }

  "the system" should "provide denormalized posts" in {
    val streamsBuilder = new StreamsBuilder()
    val dataProcessing = new DataProcessing(streamsBuilder)
    dataProcessing.produceDenormalizedPosts()
    val topology = streamsBuilder.build()
    val driver = new TopologyTestDriver(topology, configForTest, 0L)

    val userFactory = new ConRecFactory[Id[User], User]
    val user1 = User(Id("user1"), Instant.now().minusSeconds(200000), new URI("http://user1"), "nick1", false, false)
    println(s"user1: $user1")
    driver.pipeInput(userFactory.create(user1))

    val postFactory = new ConRecFactory[Id[Post], Post]
    val post1 = Post(Id("post1"), Instant.now().minusSeconds(100000), user1.id, "message", new URI("http://post1"), false)
    driver.pipeInput(postFactory.create(post1))

    val denormalizedPostReader = new ProdRecFactory[Id[Post], DenormalisedPost](driver)
    val denormalizedPost = denormalizedPostReader.readOutput().get
    assert(denormalizedPost.value.author == user1)
    assert(denormalizedPost.value.post == post1)
    assert(denormalizedPost.value.interactions == DenormalisedPost.Interactions(Set.empty, 0))
  }
}
