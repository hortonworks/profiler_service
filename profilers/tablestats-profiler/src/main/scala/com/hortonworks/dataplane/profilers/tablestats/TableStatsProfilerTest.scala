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

package com.hortonworks.dataplane.profilers.tablestats

import org.apache.spark.sql.SparkSession
import org.json4s.{JArray, JValue}

object TableStatsProfilerTest extends App {

//    val session = SparkSession.builder().master("local[*]").appName("TableStatsProfiler").getOrCreate()
//    val tableDF = session.read.json("/Users/gnagar/Downloads/20170623")
//    val metrics = TableStats.profileDF(tableDF)

  val database = args(0)
  val table = args(1)
  val metastoreUrl = args(2)

  val hiveContext = SparkSession.builder().appName("ColumnProfiler")
    .config("hive.metastore.uris", metastoreUrl)
    .enableHiveSupport().getOrCreate()

  val tableDF = TableStats.getTableDF(hiveContext, database, table, 100)
  val metrics = TableStats.profileDF(tableDF)

  import org.json4s.DefaultReaders._
  import org.json4s.jackson.JsonMethods._

  println(pretty(mergeJson(metrics.toJson)))

  Thread.sleep(100000)

  def mergeJson(jArray: JArray): JValue = {
    jArray.as[Seq[JValue]].reduce(_.merge(_))
  }

}
