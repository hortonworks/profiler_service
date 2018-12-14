import sbt.Keys.mappings

name := """audit-profiler"""

Common.settings

enablePlugins(JavaAppPackaging)

mainClass in Compile := Some("AuditProfiler")

val sparkVersion = Common.sparkVersion

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.scalactic" %% "scalactic" % "3.0.4" % "test",
  "org.scalatest" %% "scalatest" % "3.0.4" % "test",
  "com.hortonworks.hive" %% "hive-warehouse-connector" % "1.0.0.3.0.1.0-136" % "provided" excludeAll (
    ExclusionRule("org.apache.hadoop", "hadoop-aws")
    )
)


sources in EditSource <++= baseDirectory.map(
  d => (d / "src" ** "meta.json").get
)

sources in EditSource <++= baseDirectory.map(
  d => (d / "src" ** "audit-profiler.conf").get
)

sources in EditSource <++= baseDirectory.map(
  d => (d / "src" ** "audit-current-profiler.conf").get
)

sources in EditSource <++= baseDirectory.map(
  d => (d / "src" ** "audit-selector.conf").get
)

sources in EditSource <++= baseDirectory.map(
  d => (d / "src" ** "audit-current-selector.conf").get
)

targetDirectory in EditSource <<= baseDirectory(_ / "target")

variables in EditSource += ("version", version.value)

compile in Compile <<= (compile in Compile) dependsOn (edit in EditSource)

packageName in Universal := s"${name.value}-${Common.hdpVersionIdentifier}-${version.value}"

val jarsToPack = Seq(
  "com.hortonworks.dataplane.audit-profiler"
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
  file("profilers/audit-profiler/target/meta.json") -> "manifest/meta.json",
  file("profilers/audit-profiler/target/audit-profiler.conf") -> "manifest/audit-profiler.conf",
  file("profilers/audit-profiler/target/audit-current-profiler.conf") -> "manifest/audit-current-profiler.conf",
  file("profilers/audit-profiler/target/audit-selector.conf") -> "manifest/audit-selector.conf",
  file("profilers/audit-profiler/target/audit-current-selector.conf") -> "manifest/audit-current-selector.conf",
  file("COPYING") -> "legal/COPYING",
  file("third_party_components.txt") -> "legal/third_party_components.txt",
  file("internal/LICENSE") -> "legal/LICENSE"
)