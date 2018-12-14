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

package com.hortonworks.dataplane.profilers.auditprofiler

import java.time.{Instant, LocalDateTime, ZoneId}

object InputParser {

  import org.json4s.DefaultReaders._
  import org.json4s._
  import org.json4s.jackson.JsonMethods._

  def get(data: String): (Seq[AuditQueryJob], Map[String, String]) = {
    val json = parse(data)
    val configs = (json \ "clusterconfigs").as[Map[String, String]]
    val job = (json \ "assets").asInstanceOf[JArray].arr.map(getJob)
    (job, configs)
  }

  private def getJob(job: JValue): AuditQueryJob = {
    val agt = (job \ "aggregationType").as[String]
    val sameDay = (job \ "sameDay").as[Option[Boolean]].getOrElse(false)

    val dayToProcess = if (sameDay) {
      System.currentTimeMillis()
    } else System.currentTimeMillis() - 24 * 60 * 60 * 1000

    val ed = (job \ "endDate").as[Option[Long]].getOrElse(dayToProcess)

    val aggregationType = AggregationType.withName(agt)
    val endTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(ed), ZoneId.systemDefault())

    val queries =
      (job \ "queries").asInstanceOf[JArray].arr.map {
        jq =>
          getQuery(jq)
      }

    val source = getSource((job \ "source").as[JValue])

    AuditQueryJob(aggregationType, queries, endTime, source)
  }

  private def getSource(jq: JValue): DataSource = {
    (jq \ "type").as[String] match {
      case "assetrollup" =>
        val metric = (jq \ "metric").as[String]
        AssetRollup(metric)
      case "rangeraudit" =>
        val asset = (jq \ "asset").as[String]
        RangerAuditRaw(asset)
    }
  }

  private def getQuery(jq: JValue) = {
    (jq \ "queryType").as[String] match {
      case "topk" => getTopKQuery(jq)
      case "sql" => getSqlQuery(jq)
      case "hiveaggregate" => getHiveAggregateQuery(jq)
      case "hdfsaggregate" => getHdfsAggregateQuery(jq)
    }
  }

  private def getTopKQuery(q: JValue): Query = {
    val key = (q \ "key").as[String]
    val outJs = (q \ "output").as[String]
    val field = (q \ "field").as[String]
    val no = (q \ "no").as[Int]
    val filter = (q \ "filter").as[Option[String]]
    TopKQuery(key, field, no, filter)
  }

  private def getSqlQuery(q: JValue): Query = {
    val key = (q \ "key").as[String]
    val outJs = (q \ "output").as[String]
    val sql = (q \ "sql").as[String]
    val table = (q \ "table").as[String]
    SqlQuery(key, sql, table)
  }

  private def getHiveAggregateQuery(q: JValue): Query = {
    val key = (q \ "key").as[String]
    HiveRangerAggregate(key)
  }

  private def getHdfsAggregateQuery(q: JValue): Query = {
    val key = (q \ "key").as[String]
    HdfsRangerAggregate(key)
  }
}
