name := """profiler-agent"""

Common.settings

enablePlugins(PlayScala, RpmPlugin)

val hiveMetastoreVersion = Common.hiveMetastoreVersion

libraryDependencies ++= Seq(
  "com.typesafe.play" %% "play-slick" % "2.1.0",
  "com.github.tminglei" %% "slick-pg" % "0.15.0-RC",
  "com.typesafe.play" % "play-ws_2.11" % "2.5.13",
  "com.github.tminglei" %% "slick-pg_play-json" % "0.15.0-RC",
  "com.google.guava" % "guava" % "22.0",
  "com.h2database" % "h2" % "1.4.194",
  "org.postgresql" % "postgresql" % "9.3-1100-jdbc4",
  "mysql" % "mysql-connector-java" % "5.1.34" % "provided",
  "joda-time" % "joda-time" % "2.9.9",
  "org.flywaydb" %% "flyway-play" % "3.2.0",
  "org.quartz-scheduler" % "quartz" % "2.3.0",
  "org.quartz-scheduler" % "quartz-jobs" % "2.3.0",
  "org.mockito" % "mockito-core" % "1.8.5" % Test,
  "commons-codec" % "commons-codec" % "1.4",
  "org.apache.hive" % "hive-metastore" % hiveMetastoreVersion excludeAll(
    ExclusionRule(organization = "org.apache.logging.log4j"),
    ExclusionRule(organization = "org.slf4j"),
    ExclusionRule(organization = "ch.qos.logback"),
    ExclusionRule(organization = "javax.mail"),
    ExclusionRule("javax.servlet","servlet-api"),
    ExclusionRule("javax.servlet","jsp-api"),
    ExclusionRule("javax.servlet.jsp","jsp-api"),
    ExclusionRule("org.mortbay.jetty","servlet-api"),
    ExclusionRule("org.apache.hadoop", "hadoop-aws")
  ),
  "org.apache.hadoop" % "hadoop-mapred" % "0.22.0" intransitive(),
  "org.scalatestplus.play" %% "scalatestplus-play" % "3.1.2" % Test
)

rpmBrpJavaRepackJars := true

maintainer in Linux := "First Lastname <first.last@example.com>"

packageSummary in Linux := "Dataplane Profiler Agent"

packageDescription := "Dataplane Profiler Agent"

rpmRelease := "1"

rpmGroup := Some("dataplane")

rpmVendor := "hortonworks.com"

rpmUrl := Some("http://github.com/hortonworks/dataplane_profilers")

rpmLicense := Some("Hortonworks")

defaultLinuxInstallLocation := "/var/lib"

daemonUser in Linux := "dpprofiler"

daemonGroup in Linux := "dpprofiler"


mappings in Universal ++= Seq(
  file("COPYING") -> "legal/COPYING",
  file("third_party_components.txt") -> "legal/third_party_components.txt",
  file("internal/LICENSE") -> "legal/LICENSE"
)
