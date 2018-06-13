name := "hive-profiler"

enablePlugins(JavaAppPackaging)

mainClass in Compile := Some("ProfileTable")

Common.settings

val atlasVersion = Common.atlasVersion

val sparkVersion = Common.sparkVersion

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.apache.atlas" % "atlas-typesystem" % atlasVersion,
  "org.apache.atlas" % "atlas-common" % atlasVersion,
  "org.apache.atlas" % "atlas-intg" % atlasVersion,
  "org.scalaj" %% "scalaj-http" % "1.1.4",
  "org.json4s" % "json4s-native_2.11" % "3.2.11",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
  "ch.qos.logback" % "logback-classic" % "1.1.2"
)

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

sources in EditSource <++= baseDirectory.map(
  d => (d / "src" ** "meta.json").get
)

sources in EditSource <++= baseDirectory.map(
  d => (d / "src" ** "hivecolumn-profiler.conf").get
)

sources in EditSource <++= baseDirectory.map(
  d => (d / "src" ** "hivecolumn-selector.conf").get
)

sources in EditSource <++= baseDirectory.map(
  d => (d / "src" ** "hivecolumnsample-profiler.conf").get
)

targetDirectory in EditSource <<= baseDirectory(_ / "target")

variables in EditSource += ("atlasVersion", atlasVersion)
variables in EditSource += ("version", version.value)

compile in Compile <<= (compile in Compile) dependsOn (edit in EditSource)

val jarsToPack= Seq(
  "com.hortonworks.dataplane.hive-profiler",
  "org.json4s.json4s-native",
  "org.apache.atlas.atlas-common",
  "org.apache.atlas.atlas-typesystem",
  "org.apache.atlas.atlas-intg",
  "org.scalaj.scalaj-http",
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
  file("profilers/hive-profiler/target/meta.json") -> "manifest/meta.json",
  file("profilers/hive-profiler/target/hivecolumn-profiler.conf") -> "manifest/hivecolumn-profiler.conf",
  file("profilers/hive-profiler/target/hivecolumn-selector.conf") -> "manifest/hivecolumn-selector.conf",
  file("profilers/hive-profiler/target/hivecolumnsample-profiler.conf") -> "manifest/hivecolumnsample-profiler.conf"
)