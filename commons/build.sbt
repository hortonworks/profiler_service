name := """commons"""

Common.settings

libraryDependencies ++= Seq(
  "com.typesafe.play" % "play-ws_2.11" % "2.5.13" % "provided",
  "com.typesafe.play" % "play-json_2.11" % "2.6.0-M3" % "provided",
  "org.scalaj" %% "scalaj-http" % "1.1.4"  % "provided",
  "org.json4s" % "json4s-native_2.11" % "3.2.11" % "provided",
  "org.json4s" % "json4s-jackson_2.11" % "3.2.11" % "provided"
)