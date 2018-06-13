name := """hive-metastore-profiler"""

enablePlugins(JavaAppPackaging)

mainClass in Compile := Some("MetadataProfiler")

Common.settings

val sparkVersion = Common.sparkVersion

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.json4s" % "json4s-native_2.11" % "3.2.11",
  "org.scalactic" %% "scalactic" % "3.0.4",
  "org.scalatest" %% "scalatest" % "3.0.4" % "test",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
  "ch.qos.logback" % "logback-classic" % "1.1.2"
)

logBuffered in Test := false
publishArtifact in (Test, packageBin) := true

sources in EditSource <++= baseDirectory.map(
  d => (d / "src" ** "meta.json").get
)

sources in EditSource <++= baseDirectory.map(
  d => (d / "src" ** "canned-profiler.conf").get
)

sources in EditSource <++= baseDirectory.map(
  d => (d / "src" ** "canned-selector.conf").get
)

targetDirectory in EditSource <<= baseDirectory(_ / "target")

variables in EditSource += ("version", version.value)

compile in Compile <<= (compile in Compile) dependsOn (edit in EditSource)

val jarsToPack= Seq(
  "com.hortonworks.dataplane.hive-metastore-profiler",
  "org.json4s.json4s-native",
  "ch.qos.logback.logback-classic",
  "com.typesafe.scala-logging.scala-logging",
  "com.hortonworks.dataplane.commons"
)

mappings in Universal := {
  // universalMappings: Seq[(File,String)]
  val universalMappings = (mappings in Universal).value
  // removing means filtering
  // notice the "!" - it means NOT, so only keep those that do NOT have a name ending with "jar"
  val filtered = universalMappings filter {
    case (file, name) => {
      jarsToPack.filter(e => name.startsWith(s"lib/${e}")).size > 0
    }
  }
  filtered
}

mappings in Universal ++= Seq(
  file("profilers/hive-metastore-profiler/target/meta.json") -> "manifest/meta.json",
  file("profilers/hive-metastore-profiler/target/canned-profiler.conf") -> "manifest/canned-profiler.conf",
  file("profilers/hive-metastore-profiler/target/canned-selector.conf") -> "manifest/canned-selector.conf",
  file("COPYING") -> "legal/COPYING",
  file("third_party_components.txt") -> "legal/third_party_components.txt",
  file("internal/LICENSE") -> "legal/LICENSE"
)
