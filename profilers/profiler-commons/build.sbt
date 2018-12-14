

name := """profiler-commons"""

Common.settings

val hadoopVersion = Common.hadoopVersion
val sparkVersion = Common.sparkVersion

libraryDependencies ++= Seq(
  "org.scalaj" %% "scalaj-http" % "1.1.4" % "provided",
  "org.json4s" % "json4s-native_2.11" % "3.2.11" % "provided",
  "org.json4s" % "json4s-jackson_2.11" % "3.2.11" % "provided",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
  "ch.qos.logback" % "logback-classic" % "1.1.2",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"
)