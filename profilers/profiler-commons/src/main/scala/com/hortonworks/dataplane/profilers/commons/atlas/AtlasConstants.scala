/*
 HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES


  (c) 2016-2018 Hortonworks, Inc. All rights reserved.

  This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
  Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
  to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
  properly licensed third party, you do not have any rights to this code.

  If this code is provided to you under the terms of the AGPLv3:
  (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
  (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
    LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
  (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
    FROM OR RELATED TO THE CODE; AND
  (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
    DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
    DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
    OR LOSS OR CORRUPTION OF DATA.
*/
package com.hortonworks.dataplane.profilers.commons.atlas

import java.nio.charset.StandardCharsets
import java.util.Base64

object AtlasConstants {
  val Name: String = "name"
  val QualifiedName: String = "qualifiedName"
  val ClusterName: String = "clusterName"
  val HiveTableTypeName: String = "hive_table"
  val HiveDbTypeName: String = "hive_db"
  val HiveColumnTypeName: String = "hive_column"
  val HiveSdTypeName: String = "hive_storagedesc"
  val HiveColumnProfileData: String = "dss_hive_column_profile_data"
  val HiveTableProfileData: String = "dss_hive_table_profile_data"
  val ProfileDataAttr: String = "profileData"
  val DistributionData: String = "distributionData"
  val ColumnsAtttribute: String = "columns"
  val DbAttribute: String = "db"
  val TableAttribute: String = "table"
  val TypeAttribute: String = "type"
  val TableSdAttribute: String = "sd"

  val AtlasPostRequestHeaders: List[(String, String)] = List(("X-XSRF-HEADER", "valid"),
    ("Content-Type", "application/json"))
  val atlasEntityRESTEndpoint: String = "/api/atlas/entities"
  val atlasEntityPartialUpdateRESTEndpoint: String = atlasEntityRESTEndpoint + "/qualifiedName"

  def getEncodedAuthorizationHeader(atlasUser: String, atlasPassword: String): String = {
    "Basic " + Base64.getEncoder.encodeToString((atlasUser + ":" +
      atlasPassword).getBytes(StandardCharsets.UTF_8))
  }

  def getDbQualifiedName(dbName: String, clusterName: String): String = {
    dbName + " @" + clusterName
  }

  def getTableQualifiedName(tableName: String, dbName: String, clusterName: String): String = {
    dbName + "." + tableName + "@" + clusterName
  }

  def getColumnQualifiedName(colName: String, tableName: String, dbName: String, clusterName: String): String = {
    dbName + "." + tableName + "." + colName + "@" + clusterName
  }
}