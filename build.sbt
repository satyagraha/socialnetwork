scalacOptions += "-Ypartial-unification"
resolvers += Opts.resolver.sonatypeSnapshots

lazy val kafkaVersion = "2.3.0"

libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka-streams-scala" % kafkaVersion,
  "org.typelevel" %% "cats-core" % "1.2.0",
  "com.goyeau" %% "kafka-streams-circe" % "0.5",
  "ch.qos.logback" % "logback-classic" % "1.2.3"
) ++ circe ++ monocle ++ testDependencies

lazy val testDependencies = Seq(
  "org.apache.kafka" % "kafka-streams-test-utils" % kafkaVersion % Test,
  "org.scalatest" %% "scalatest" % "3.0.8" % Test
)

lazy val circe = {
  val version = "0.11.1"
  Seq(
    "io.circe" %% "circe-core" % version,
    "io.circe" %% "circe-parser" % version,
    "io.circe" %% "circe-java8" % version,
    "io.circe" %% "circe-generic" % version
  )
}

lazy val monocle = {
  val monocleVersion = "1.5.1-cats"
  Seq(
    "com.github.julien-truffaut" %% "monocle-core" % monocleVersion,
    "com.github.julien-truffaut" %% "monocle-macro" % monocleVersion
  )
}
