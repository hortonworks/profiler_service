name := "geo-profiler"

Common.settings

val atlasVersion = Common.atlasVersion

val sparkVersion = Common.sparkVersion

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.apache.atlas" % "atlas-client" % atlasVersion ,
  "org.apache.atlas" % "atlas-typesystem" % atlasVersion,
  "org.apache.atlas" % "atlas-intg" % atlasVersion,
  "org.apache.atlas" % "atlas-common" % atlasVersion,
  "org.scalaj" %% "scalaj-http" % "1.1.4",
  "org.json4s" % "json4s-native_2.11" % "3.2.11",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
  "ch.qos.logback" % "logback-classic" % "1.1.2",
  "org.apache.opennlp" % "opennlp-tools" % "1.8.0",
  "com.rockymadden.stringmetric" %% "stringmetric-core" % "0.27.4"
)

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
