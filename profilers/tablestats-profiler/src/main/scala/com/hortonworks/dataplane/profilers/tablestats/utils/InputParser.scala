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

package com.hortonworks.dataplane.profilers.tablestats.utils

import com.hortonworks.dataplane.profilers.commons.parse.{ParseKerberosCredentials, KerberosCredentials}

object InputParser {

  import org.json4s.DefaultReaders._
  import org.json4s._
  import org.json4s.jackson.JsonMethods._
  import org.json4s.JValue

  def get(input: String): InputArgs = {
    val json = parse(input)
    val tables = (json \ "assets").asInstanceOf[JArray].arr.map(getTableInfo)

    val clusterConfigs = (json \ "clusterconfigs").as[JValue]
    val atlasURL = (clusterConfigs \ "atlasUrl").as[String]
    val atlasUser = (clusterConfigs \ "atlasUser").as[String]
    val atlasPassword = (clusterConfigs \ "atlasPassword").as[String]
    val clusterName = (clusterConfigs \ "clusterName").as[String]
    val metastoreUrl = (clusterConfigs \ "metastoreUrl").as[String]
    val samplePercent = (json \ "jobconf" \ "samplepercent").as[String].toDouble

    val kerberosCredentials = ParseKerberosCredentials.getkerberosCredentials(json)

    val atlasInfo = AtlasInfo(atlasURL, atlasUser, atlasPassword)

    InputArgs(metastoreUrl, tables, clusterName, atlasInfo, samplePercent, kerberosCredentials)
  }

  def getTableInfo(jq: JValue): TableInfo = {
    val database = (jq \ "db").as[String]
    val table = (jq \ "table").as[String]
    TableInfo(database, table)
  }

}

case class TableInfo(database: String, table: String)

case class AtlasInfo(url: String, user: String, password: String) {
  override def toString: String = s"AtlasInfo(${url}, ${user}, ******)"
}

case class InputArgs(metastoreUrl: String, tables: Seq[TableInfo], clusterName: String, atlasInfo: AtlasInfo,
                     samplePercent: Double, kerberosCredentials: KerberosCredentials)
