import sbt.ExclusionRule

name := """profiler-agent"""

Common.settings

enablePlugins(PlayScala, RpmPlugin)

val hiveMetastoreVersion = Common.hiveMetastoreVersion

val hadoopVersion = Common.hadoopVersion

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
  "org.quartz-scheduler" % "quartz" % "2.3.0",
  "org.quartz-scheduler" % "quartz-jobs" % "2.3.0",
  "org.mockito" % "mockito-core" % "2.1.0" % Test,
  "commons-codec" % "commons-codec" % "1.4",
  "org.apache.hive" % "hive-metastore" % hiveMetastoreVersion excludeAll(
    ExclusionRule(organization = "org.apache.logging.log4j"),
    ExclusionRule(organization = "org.slf4j"),
    ExclusionRule(organization = "ch.qos.logback"),
    ExclusionRule(organization = "javax.mail"),
    ExclusionRule("javax.servlet", "servlet-api"),
    ExclusionRule("javax.servlet", "jsp-api"),
    ExclusionRule("javax.servlet.jsp", "jsp-api"),
    ExclusionRule("org.mortbay.jetty", "servlet-api"),
    ExclusionRule("org.apache.hadoop", "hadoop-aws"),
    ExclusionRule("javax.activation", "activation"),
    ExclusionRule("net.jcip", "jcip-annotations"),
    ExclusionRule("org.json", "json"),
    ExclusionRule("com.google.code.findbugs", "jsr305"),
    ExclusionRule("junit", "junit"),
    ExclusionRule("org.codehaus.jackson", "jackson-core-asl"),
    ExclusionRule("antlr", "antlr"),
    ExclusionRule("org.apache.arrow", "arrow-memory"),
    ExclusionRule("org.apache.arrow", "arrow-vector")
  ),
  "org.flywaydb" % "flyway-core" % "5.0.7",
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVersion excludeAll(
    ExclusionRule("javax.activation", "activation"),
    ExclusionRule(organization = "org.apache.logging.log4j"),
    ExclusionRule(organization = "org.slf4j"),
    ExclusionRule(organization = "ch.qos.logback"),
    ExclusionRule("org.apache.arrow", "arrow-memory"),
    ExclusionRule("org.apache.arrow", "arrow-vector")
  ),
  "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion excludeAll(
    ExclusionRule("javax.activation", "activation"),
    ExclusionRule(organization = "org.apache.logging.log4j"),
    ExclusionRule(organization = "org.slf4j"),
    ExclusionRule(organization = "ch.qos.logback"),
    ExclusionRule("org.apache.arrow", "arrow-memory"),
    ExclusionRule("org.apache.arrow", "arrow-vector")
  ),
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion excludeAll(
    ExclusionRule("javax.activation", "activation"),
    ExclusionRule(organization = "org.apache.logging.log4j"),
    ExclusionRule(organization = "org.slf4j"),
    ExclusionRule(organization = "ch.qos.logback"),
    ExclusionRule("org.apache.arrow", "arrow-memory"),
    ExclusionRule("org.apache.arrow", "arrow-vector")
  ),
  "org.scalatestplus.play" %% "scalatestplus-play" % "2.0.0" % Test,
  "com.nimbusds" % "nimbus-jose-jwt" % "3.9"

)


excludeDependencies += "com.google.code.findbugs"

excludeDependencies += "net.jcip" % "jcip-annotations"

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

packageName in Universal := s"${name.value}-${Common.hdpVersionIdentifier}-${version.value}"

mappings in Universal ++= Seq(
  file("COPYING") -> "legal/COPYING",
  file("third_party_components.txt") -> "legal/third_party_components.txt",
  file("internal/LICENSE") -> "legal/LICENSE"
)
