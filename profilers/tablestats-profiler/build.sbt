name := """tablestats-profiler"""

Common.settings

enablePlugins(JavaAppPackaging)
mainClass in Compile := Some("com.hortonworks.dataplane.profilers.tablestats.TableStatsProfiler")


val sparkVersion = Common.sparkVersion
val atlasVersion = Common.atlasVersion

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.apache.atlas" % "atlas-typesystem" % atlasVersion excludeAll (
    ExclusionRule("javax.activation", "activation")
    ),
  "org.apache.atlas" % "atlas-common" % atlasVersion excludeAll (
    ExclusionRule("javax.activation", "activation")
    ),
  "org.apache.atlas" % "atlas-intg" % atlasVersion excludeAll (
    ExclusionRule("javax.activation", "activation")
    ),
  "org.scalactic" %% "scalactic" % "3.0.4" % "test",
  "org.scalatest" %% "scalatest" % "3.0.4" % "test",
  "org.scalaj" %% "scalaj-http" % "1.1.4",
  "org.json4s" % "json4s-native_2.11" % "3.2.11",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
  "ch.qos.logback" % "logback-classic" % "1.1.2",
  "com.hortonworks.hive" %% "hive-warehouse-connector" % "1.0.0.3.0.1.0-136" % "provided" excludeAll (
    ExclusionRule("org.apache.hadoop", "hadoop-aws")
    )
)


resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

sources in EditSource <++= baseDirectory.map(
  d => (d / "src" ** "meta.json").get
)

sources in EditSource <++= baseDirectory.map(
  d => (d / "src" ** "tablestats-profiler.conf").get
)


sources in EditSource <++= baseDirectory.map(
  d => (d / "src" ** "tablestats-selector.conf").get
)


targetDirectory in EditSource <<= baseDirectory(_ / "target")

variables in EditSource += ("atlasVersion", atlasVersion)
variables in EditSource += ("version", version.value)
variables in EditSource += ("hdpInterfaceVersion", Common.hdpInterfaceVersion)

compile in Compile <<= (compile in Compile) dependsOn (edit in EditSource)

packageName in Universal := s"${name.value}-${Common.hdpVersionIdentifier}-${version.value}"

val jarsToPack = Seq(
  "com.hortonworks.dataplane.tablestats-profiler",
  "org.json4s.json4s-native",
  "org.apache.atlas.atlas-common",
  "org.apache.atlas.atlas-typesystem",
  "org.apache.atlas.atlas-intg",
  "org.scalaj.scalaj-http",
  "ch.qos.logback.logback-classic",
  "com.typesafe.scala-logging.scala-logging",
  "com.hortonworks.dataplane.commons",
  "com.hortonworks.dataplane.profiler-commons",
  "com.hortonworks.dataplane.hdp-interface-definition",
  "com.hortonworks.dataplane.hdp-interfaces"
)

test in assembly := {}

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
  file("profilers/tablestats-profiler/target/meta.json") -> "manifest/meta.json",
  file("profilers/tablestats-profiler/target/tablestats-profiler.conf") -> "manifest/tablestats-profiler.conf",
  file("profilers/tablestats-profiler/target/tablestats-selector.conf") -> "manifest/tablestats-selector.conf",
  file("COPYING") -> "legal/COPYING",
  file("third_party_components.txt") -> "legal/third_party_components.txt",
  file("internal/LICENSE") -> "legal/LICENSE"
)