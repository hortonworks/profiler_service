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

import AtlasPersister.logger
import ProfileTable.logger
import org.apache.atlas.typesystem.json.InstanceSerialization
import org.apache.atlas.typesystem.{Referenceable, Struct}

import scala.collection.mutable.Map
import org.json4s.native.Json
import org.json4s.DefaultFormats
import java.util.Base64
import java.nio.charset.StandardCharsets

/**
  * Created by visharma on 6/7/17.
  * Test Script to prototype working of Atlas persistence part of the profilers
  */





object ScalaTesting extends App{
  val arr = List((100, 1000), (1000, 1000))
  val arrCase = arr.map(x => Utils.HistogramEntries(x._1.toString, x._2.toString.toLong))
  println(Json(DefaultFormats).writePretty(arrCase))

  //var metric = Metrics("", "", Option(""), "", "", "", "", "", "", "", "", "", "", "", "", "", "", "")
  //metric.cardinality = "1000"
}

object AtlasRESTAuthorization extends App{
  val atlasResource = "http://ctr-e134-1499953498516-141450-01-000002.hwx.site:21000/api/atlas/entities"
  val atlasUser = "admin"
  val atlasPassword = "admin"
  val authString = "Basic " + Base64.getEncoder.encodeToString((atlasUser + ":" + atlasPassword).getBytes(StandardCharsets.UTF_8))
  val tableGetParams = List((Constants.TypeAttribute, Constants.HiveTableTypeName), ("property", "qualifiedName"), ("value", "tpcds_text_1000.promotion@profiler_perf"))
  val getTableResponse = AtlasInterface.getData(atlasResource, ("Authorization", authString) :: Constants.AtlasPostRequestParams, tableGetParams)
  println(getTableResponse.body)

}

object AtlasTest extends App {
  val atlasUrl = "http://172.27.21.145:21000/api/atlas/entities"
  val clusterName = "randomCluster"
  val dbName = "randomDB"
  val tableName = "randomTable"

  val metrics = Map[String, Map[String, String]]()
  metrics.put("id", Map(("type" -> "int"), ("min" -> "100000"), ("mean" -> "50000")))
  metrics.put("name", Map(("type" -> "string"), ("max" -> "10000000"), ("count" -> "5000000")))

  //AtlasPersister.persistToAtlas(atlasUrl, clusterName, dbName, tableName, metrics, 1000)
}
