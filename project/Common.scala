/*
 *   HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
 *
 *   (c) 2016-2018 Hortonworks, Inc. All rights reserved.
 *
 *   This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
 *   Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
 *   to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
 *   properly licensed third party, you do not have any rights to this code.
 *
 *   If this code is provided to you under the terms of the AGPLv3:
 *   (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
 *   (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
 *     LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
 *   (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
 *     FROM OR RELATED TO THE CODE; AND
 *   (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
 *     DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
 *     DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
 *     OR LOSS OR CORRUPTION OF DATA.
 */

import sbt.Keys.{version, _}
import sbt._

object Common {

  val repoUrl = sys.props.getOrElse("repourl", "http://nexus-private.hortonworks.com/nexus/content/groups/public")

  val hdpInterfaceVersion = sys.props.getOrElse("hdp.interface.version", "2.6-0.1")

  val hdpInterfaceProjectName = Map("2.6-0.1" -> "hdp-interfaces26", "3.0-0.1" -> "hdp-interfaces30")(hdpInterfaceVersion)

  val hdpVersionIdentifier = Map("2.6-0.1" -> "hdp2", "3.0-0.1" -> "hdp3")(hdpInterfaceVersion)

  val sparkVersion = sys.props.getOrElse("spark.version", "2.2.0.2.6.3.0-235")

  val hadoopVersion = sys.props.getOrElse("hadoop.version", "2.7.3")

  val atlasVersion = sys.props.getOrElse("atlas.version", "0.8.0.2.6.5.3001-10")

  val hiveMetastoreVersion = sys.props.getOrElse("hive.metastore.version", "2.1.0.2.6.3.0-235")

  val settings: Seq[Setting[_]] = Seq(
    organization := "com.hortonworks.dataplane",
    version := sys.props.getOrElse("dss.version", "1.0.0-SNAPSHOT"),
    scalaVersion := "2.11.11",

    resolvers += "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository",
    resolvers += Resolver.file("Local repo", file(System.getProperty("user.home") + "/.ivy2/local"))(Resolver.ivyStylePatterns),
    resolvers += "Additional Maven Repository" at repoUrl,
    resolvers += "Hortonworks Maven Repository" at "http://repo.hortonworks.com/content/groups/public/"
  )
}
