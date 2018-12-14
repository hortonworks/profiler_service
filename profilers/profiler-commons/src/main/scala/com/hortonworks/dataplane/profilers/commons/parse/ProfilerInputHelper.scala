/*
 * HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
 * (c) 2016-2018 Hortonworks, Inc. All rights reserved.
 * This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
 * Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
 * to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
 * properly licensed third party, you do not have any rights to this code.
 * If this code is provided to you under the terms of the AGPLv3:
 * (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
 * (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
 *   LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
 * (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
 *   FROM OR RELATED TO THE CODE; AND
 * (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
 *   DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
 *   DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
 *   OR LOSS OR CORRUPTION OF DATA.
 */

package com.hortonworks.dataplane.profilers.commons.parse

import com.hortonworks.dataplane.profilers.commons.security.ProfilerCredentialProvider
import org.apache.spark.sql.SparkSession

trait ProfilerInputHelper {

  self: ProfilerInput =>

  import org.json4s.DefaultReaders._
  import org.json4s._

  implicit val jsonDefaultFormats = DefaultFormats

  def getAtlasInfo() = {
    val url = (self.clusterconfigs \ "atlasUrl").extract[String]
    val user = (self.clusterconfigs \ "atlasUser").extract[String]
    val password = (self.clusterconfigs \ "atlasPassword").extract[String]
    val clusterName = (self.clusterconfigs \ "clusterName").extract[String]

    val credentialStoreEnabled = (self.clusterconfigs \ "credential.store.enabled").extract[String].toBoolean

    val decryptedPassword = if (credentialStoreEnabled) {
      val credFile = (self.clusterconfigs \ "credential.provider.path").extract[String]
      val credProvider = new ProfilerCredentialProvider(credFile)
      credProvider.resolveAlias(password)
    } else password

    AtlasInfo(url, user, decryptedPassword, clusterName)
  }

  def sparkConfigurations: Map[String, String] = {
    val clusterConfigAsMap = self.clusterconfigs.extract[Map[String, JValue]]

    val sparkConfigurations = clusterConfigAsMap.filterKeys(_.startsWith("sparkConfigurations")).map(
      pair => {
        val indexOfFirstDot = pair._1.indexOf('.')
        pair._1.substring(indexOfFirstDot + 1) -> pair._2.extract[String]
      }
    )
    println(s"Identified spark configurations : $sparkConfigurations")
    sparkConfigurations
  }

  def getHiveTables(): Seq[HiveAsset] = {
    self.assets.map {
      data =>
        val database = (data \ "db").as[String]
        val table = (data \ "table").as[String]
        val partitions = (data \ "partitions").extractOpt[Seq[Map[String, String]]]
        HiveAsset(database, table, partitions.getOrElse(Nil))
    }
  }

  def getClusterConfig(key: String): JValue = {
    (self.clusterconfigs \ key)
  }

  def getJobConfig(key: String): JValue = {
    (self.jobconf \ key)
  }

}

object ProfilerInputHelper {

  implicit class SparkSessionUtils(session: SparkSession.Builder) {

    import org.json4s.DefaultReaders._
    import org.json4s._

    implicit val jsonDefaultFormats = DefaultFormats

    def addHiveContext(input: ProfilerInput): SparkSession.Builder = {
      val isSecured = (input.clusterconfigs \ "isSecured").as[String]
      val metastoreUrl = (input.clusterconfigs \ "metastoreUrl").as[String]

      val kerberosCredentials = isSecured match {
        case "true" => {
          val metastoreKeytab = (input.clusterconfigs \ "metastoreKeytab").as[String]
          val metastorePrincipal = (input.clusterconfigs \ "metastorePrincipal").as[String]
          session
            .config("hive.metastore.uris", metastoreUrl)
            .config("hive.metastore.kerberos.keytab", metastoreKeytab)
            .config("hive.metastore.kerberos.principal", metastorePrincipal)
            .config("hive.metastore.sasl.enabled", true)
            .enableHiveSupport()
        }
        case _ =>
          session
            .config("hive.metastore.uris", metastoreUrl)
            .enableHiveSupport()
      }
      session
    }
  }

}
