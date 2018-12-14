import sbt.{ExclusionRule, Resolver}

name := "hdp-interfaces"

organization := "com.hortonworks.dataplane"

version := "3.0-0.1"

scalaVersion := "2.11.11"

val sparkVersion = "2.3.1.3.0.1.0-136"

val repoUrl = sys.props.getOrElse("repourl", "http://nexus-private.hortonworks.com/nexus/content/groups/public")

resolvers += Resolver.file("Local repo", file(System.getProperty("user.home") + "/.ivy2/local"))(Resolver.ivyStylePatterns)
resolvers += "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository"
resolvers += "Additional Maven Repository" at repoUrl
resolvers += "Hortonworks Maven Repository" at "http://repo.hortonworks.com/content/groups/public/"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "com.hortonworks.hive" %% "hive-warehouse-connector" % "1.0.0.3.0.1.0-136" % "provided" excludeAll (
    ExclusionRule("org.apache.hadoop", "hadoop-aws")
    ),
  "org.apache.hive" % "hive-metastore" % "3.1.0.3.0.1.0-136" % "provided" excludeAll(
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