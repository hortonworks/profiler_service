name := """dataplane-profiler"""

Common.settings

lazy val commons = project in file("commons")

lazy val hdpInterfaceDefinition = project in file("hdp-interface-definition")

lazy val hdpInterface = (project in file(s"hdp-interfaces/${Common.hdpInterfaceProjectName}")).dependsOn(hdpInterfaceDefinition)

lazy val profilerCommons = (project in file("profilers/profiler-commons")).dependsOn(commons,hdpInterface)

lazy val profilerAgent = (project in file("profiler-agent")).enablePlugins(PlayScala)
  .dependsOn(commons,hdpInterface)

lazy val hiveMetastoreProfiler = (project in file("profilers/hive-metastore-profiler")).dependsOn(profilerCommons)

lazy val atlasCommons = project in file("profilers/atlas-commons")

lazy val auditProfiler = (project in file("profilers/audit-profiler")).dependsOn(profilerCommons)

lazy val sensitiveInfoProfiler = (project in file("profilers/sensitive-info-profiler")).dependsOn(atlasCommons).dependsOn(profilerCommons)

lazy val tablestatsProfiler = (project in file("profilers/tablestats-profiler")).dependsOn(atlasCommons).dependsOn(profilerCommons)