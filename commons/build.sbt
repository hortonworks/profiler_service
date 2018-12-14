name := """commons"""

Common.settings

val hadoopVersion = Common.hadoopVersion

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "provided"
)