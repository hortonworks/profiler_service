name := """hive-asset-service"""

Common.settings

enablePlugins(JavaAppPackaging)

mainClass in Compile := Some("com.hortonworks.dataplane.profilers.asset.HiveAssetService")

val atlasVersion = Common.atlasVersion
val hiveMetastoreVersion = Common.hiveMetastoreVersion

libraryDependencies ++= Seq(
  "com.google.inject" % "guice" % "4.1.0",
  "com.typesafe.play" % "play-ws_2.11" % "2.5.13",
  "com.typesafe.play" % "play-json_2.11" % "2.6.0-M3",
  "com.typesafe.akka" %% "akka-http" % "10.0.5",
  "org.apache.atlas" % "atlas-client" % atlasVersion excludeAll(
    ExclusionRule(organization = "org.apache.logging.log4j"),
    ExclusionRule(organization = "org.slf4j"),
    ExclusionRule(organization = "ch.qos.logback")
  ),
  "org.apache.atlas" % "atlas-typesystem" % atlasVersion excludeAll(
    ExclusionRule(organization = "org.apache.logging.log4j"),
    ExclusionRule(organization = "org.slf4j"),
    ExclusionRule(organization = "ch.qos.logback")
  ),
  "org.apache.atlas" % "atlas-notification" % atlasVersion excludeAll(
    ExclusionRule(organization = "org.apache.logging.log4j"),
    ExclusionRule(organization = "org.slf4j"),
    ExclusionRule(organization = "ch.qos.logback")
  ),
  "org.apache.atlas" % "atlas-intg" % atlasVersion excludeAll(
    ExclusionRule(organization = "org.apache.logging.log4j"),
    ExclusionRule(organization = "org.slf4j"),
    ExclusionRule(organization = "ch.qos.logback")
  ),
  "com.typesafe.akka" %% "akka-stream-kafka" % "0.16",
  "org.apache.hive" % "hive-metastore" % hiveMetastoreVersion excludeAll(
    ExclusionRule(organization = "org.apache.logging.log4j"),
    ExclusionRule(organization = "org.slf4j"),
    ExclusionRule(organization = "ch.qos.logback"),
    ExclusionRule("org.apache.hadoop", "hadoop-aws")
  ),
  "org.apache.hadoop" % "hadoop-mapred" % "0.22.0" intransitive(),
  "ch.qos.logback" % "logback-classic" % "1.1.7",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
)

