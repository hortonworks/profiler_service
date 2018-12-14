name := "hdp-interface-definition"

organization := "com.hortonworks.dataplane"

version := "0.1"

scalaVersion := "2.11.11"

val repoUrl = sys.props.getOrElse("repourl", "http://nexus-private.hortonworks.com/nexus/content/groups/public")

resolvers += "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository"
resolvers += "Additional Maven Repository" at repoUrl
resolvers += "Hortonworks Maven Repository" at "http://repo.hortonworks.com/content/groups/public/"

libraryDependencies ++= Seq(
  "com.typesafe.play" %% "play-json" % "2.6.9" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.2.0.2.6.3.0-235" % "provided",
  "org.apache.hive" % "hive-metastore" % "2.1.0.2.6.3.0-235" % "provided" excludeAll(
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
  )
)