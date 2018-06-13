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

import java.nio.charset.StandardCharsets
import java.util.{Base64, Calendar}

import com.hortonworks.dataplane.profilers.tablestats.utils.{ColumnMetrics, InputParser, MetricsSeq}
import com.hortonworks.dataplane.profilers.tablestats.utils.atlas.{AtlasPersister, Constants}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession
import org.json4s.{JArray, JValue}

object TableStatsProfiler extends App with LazyLogging {

  val input = InputParser.get(args(0))

  val securityCredentials = input.kerberosCredentials

  val hiveContext = securityCredentials.isSecured match {
    case "true" => {
      SparkSession.builder().appName("ColumnProfiler")
        .config("hive.metastore.uris", input.metastoreUrl).
        config("hive.metastore.kerberos.keytab", securityCredentials.keytab).
        config("hive.metastore.kerberos.principal", securityCredentials.principal).
        config("hive.metastore.sasl.enabled", true).
        enableHiveSupport().getOrCreate()
    }
    case _ => {
      SparkSession.builder().appName("ColumnProfiler")
        .config("hive.metastore.uris", input.metastoreUrl)
        .enableHiveSupport().getOrCreate()
    }
  }

  import org.json4s.DefaultReaders._
  import org.json4s.jackson.JsonMethods._

  implicit val formats = org.json4s.DefaultFormats
  logger.info("Got input => {}", input.toString)

  val atlasUrl = input.atlasInfo.url
  val atlasUser = input.atlasInfo.user
  val atlasPassword = input.atlasInfo.password
  logger.info("Got Atlas properties => Atlas URL : {}, Atlas User : {}", atlasUrl, atlasUser)

  val atlasAuthString = "Basic " + Base64.getEncoder.encodeToString((atlasUser + ":" +
    atlasPassword).getBytes(StandardCharsets.UTF_8))
  val atlasRestRequestParams = ("Authorization", atlasAuthString) :: Constants.AtlasPostRequestParams

  input.tables.foreach {
    ti => {
      val dbName = ti.database
      val tableName = ti.table
      try {
        logger.info("Hive profiler started for table {}.{}", dbName, tableName)
        val tableDF = TableStats.getTableDF(hiveContext, ti.database, ti.table, input.samplePercent)
        val metrics = TableStats.profileDF(tableDF)
        val tableRowCount = TableStats.rowCount(metrics)
        val columnMetrics = new MetricsSeq(metrics.seq.filter(_.isInstanceOf[ColumnMetrics]))
        val samplingTime = Calendar.getInstance().getTimeInMillis.toString
        val metricsJson = pretty(mergeJson(columnMetrics.toJson))
        println(metricsJson)
        val resultMap = parse(metricsJson).extract[Map[String, Map[String, Any]]]
        AtlasPersister.persistToAtlas(atlasUrl, atlasRestRequestParams, input.clusterName, dbName,
          tableName, resultMap, tableRowCount, samplingTime, input.samplePercent.toString)
        logger.info("Hive profiling for table {}.{} successful", dbName, tableName)
      } catch {
        case e: Exception => logger.error(s"Hive profiler failed for table ${dbName}.${tableName} ", e)
      }
    }
  }

  def mergeJson(jArray: JArray): JValue = {
    jArray.as[Seq[JValue]].reduce(_.merge(_))
  }

}
