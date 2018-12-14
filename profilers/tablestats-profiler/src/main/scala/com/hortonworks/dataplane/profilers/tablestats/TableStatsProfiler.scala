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

import java.util.Calendar

import com.hortonworks.dataplane.profilers.hdpinterface.impl.spark.SparkInterfaceImpl
import com.hortonworks.dataplane.profilers.hdpinterface.spark.SparkInterface
import com.hortonworks.dataplane.profilers.tablestats.TableStats.getTableDF
import com.hortonworks.dataplane.profilers.tablestats.utils._
import com.hortonworks.dataplane.profilers.tablestats.utils.atlas.AtlasPersister
import com.typesafe.scalalogging.LazyLogging
import org.json4s.{JArray, JValue, _}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}


object TableStatsProfiler extends App with LazyLogging {

  implicit val formats = org.json4s.DefaultFormats

  val inputArgs = InputParser.get(args(0))
  val assetMetricsPath = (inputArgs.input.clusterconfigs \ "assetMetricsPath").extractOpt[String].getOrElse("/user/dpprofiler/dwh")
  val isIncrementalMode = inputArgs.input.context.profilerinstancename == "tablestats-incremental"
  val savePath = s"$assetMetricsPath/tablestats/tables"

  val sparkInterface: SparkInterface = new SparkInterfaceImpl("ColumnProfiler", inputArgs.input.sparkConfigurations.toSeq)


  import org.json4s.DefaultReaders._
  import org.json4s.jackson.JsonMethods._

  logger.info("Got Atlas properties => Atlas URL : {}, Atlas User : {}", inputArgs.atlasInfo.url, inputArgs.atlasInfo.user)
  val listOfTablesAsString = inputArgs.tables.map(entry => entry.db + "." + entry.table).mkString(",")
  logger.info(s"Starting TableStats profiler for tables => ${listOfTablesAsString}")

  inputArgs.tables.foreach {
    ti => {
      val dbName = ti.db
      val tableName = ti.table
      try {
        logger.info("Hive profiler started for table {}.{}", dbName, tableName)
        logger.info(s"is tablestat in incremental mode? $isIncrementalMode")
        val metrics = if (!isIncrementalMode) TableStats.profileDFFull(getTableDF(sparkInterface, ti.db, ti.table, inputArgs.samplePercent)) else TableStats.profileDFPerPartition(sparkInterface, ti, inputArgs.samplePercent, savePath)

        //Separate out CountMetrics as that is not needed on column level
        val countAndOtherMetrics: (Seq[ColumnMetrics], Seq[ColumnMetrics]) = metrics.seq.partition(t => t.metrics match {
          case m: CountMetrics => true
          case _ => false
        })
        val tableRowCount = countAndOtherMetrics._1.headOption.map { cm =>
          cm.metrics match {
            case c: CountMetrics => Some(c.count)
            case _ => None
          }
        }.flatten

        val columnMetrics = new MetricsSeq(countAndOtherMetrics._2)
        val samplingTime = Calendar.getInstance().getTimeInMillis.toString
        val metricsJson = pretty(mergeJson(columnMetrics.toJson))
        logger.info(metricsJson)
        val resultMap = parse(metricsJson).extract[Map[String, Map[String, Any]]]
        val tableMetrics = TableMetrics(tableRowCount, inputArgs.samplePercent.toString, samplingTime)
        val persistResult = AtlasPersister.persistToAtlas(inputArgs.atlasInfo, dbName, tableName, resultMap, tableMetrics)
        //TODO - Take wait time from configs
        waitForALongPeriodForAtlasCallToComplete(tableName, persistResult, 24 * 60 * 60)

      } catch {
        case e: Exception => logger.error(s"Hive profiler failed for table ${dbName}.${tableName} ", e)
      }
    }
  }


  sparkInterface.close()

  private def waitForALongPeriodForAtlasCallToComplete(tableName: String,
                                                       eventualResult: Future[Unit], timeOutInSeconds: Int) = {
    Await.ready(eventualResult, timeOutInSeconds seconds).value match {
      case Some(mayBeResult) =>
        mayBeResult match {
          case Success(_) =>
            logger.debug(s"table  $tableName was successfully profiled")
          case Failure(error) =>
            logger.error(s"Failed to profile table $tableName ", error)
        }

      case None =>
        logger.error(s"profiling of table  $tableName timed out, waited for $timeOutInSeconds seconds")
    }
  }

  def mergeJson(jArray: JArray): JValue = {
    jArray.as[Seq[JValue]].reduce(_.merge(_))
  }

}
