name := """sensitive-info-profiler"""

Common.settings

val sparkVersion = Common.sparkVersion

enablePlugins(JavaAppPackaging)
mainClass in Compile := Some("com.hortonworks.dataplane.profilers.SensitiveProfilerApp")


libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-reflect" % "2.11.11",
  "org.scala-lang" % "scala-compiler" % "2.11.11",
  "org.scala-lang" % "scala-library" % "2.11.11",
  "com.fsist" %% "subscala" % "0.1.1",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.json4s" % "json4s-native_2.11" % "3.2.11",
  "org.scalactic" %% "scalactic" % "3.0.4",
  "org.scalatest" %% "scalatest" % "3.0.4" % "test",
  "org.scalaj" %% "scalaj-http" % "1.1.4",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
  "ch.qos.logback" % "logback-classic" % "1.1.2",
  "com.typesafe.play" %% "play-json" % "2.6.9",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.8.9" % "test",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.9" % "test",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.8.9" % "test",
  "com.hortonworks.hive" %% "hive-warehouse-connector" % "1.0.0.3.0.1.0-136" % "provided" excludeAll (
    ExclusionRule("org.apache.hadoop", "hadoop-aws")
    )
)



resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

sources in EditSource <++= baseDirectory.map(
  d => (d / "src" ** "meta.json").get
)

sources in EditSource <++= baseDirectory.map(
  d => (d / "src" ** "sensitive-info-profiler.conf").get
)

sources in EditSource <++= baseDirectory.map(
  d => (d / "src" ** "sensitive-info-selector.conf").get
)

sources in EditSource <++= baseDirectory.map(
  d => (d / "src" ** "keywords.json").get
)

sources in EditSource <++= baseDirectory.map(
  d => (d / "src" ** "labelstore.json").get
)

targetDirectory in EditSource <<= baseDirectory(_ / "target")

variables in EditSource += ("version", version.value)

compile in Compile <<= (compile in Compile) dependsOn (edit in EditSource)

mappings in(Compile, packageBin) ~= {
  _.filter(!_._1.getName.endsWith(".json"))
}


assemblyJarName in assembly := s"""${organization.value}.${name.value}-assembly-${version.value}.jar"""

test in assembly := {}


mappings in Universal := {

  val fatJar = (assembly in Compile).value
  val universalMappings = (mappings in Universal).value
  val filtered = universalMappings filter {
    case (file, name) => !name.endsWith(".jar") && !name.startsWith("bin/")
  }

  filtered :+ (fatJar -> ("lib/" + fatJar.getName))
}

packageName in Universal := s"${name.value}-${Common.hdpVersionIdentifier}-${version.value}"

mappings in Universal ++= Seq(
  file("profilers/sensitive-info-profiler/target/meta.json") -> "manifest/meta.json",
  file("profilers/sensitive-info-profiler/target/sensitive-info-profiler.conf") -> "manifest/sensitive-info-profiler.conf",
  file("profilers/sensitive-info-profiler/target/sensitive-info-selector.conf") -> "manifest/sensitive-info-selector.conf",
  file("COPYING") -> "legal/COPYING",
  file("third_party_components.txt") -> "legal/third_party_components.txt",
  file("internal/LICENSE") -> "legal/LICENSE"
)

fork := true