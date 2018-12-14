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

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.SparkSession

object AuditProfiler {

  def run(job: AuditQueryJob, properties: Map[String, String])(implicit sparkSession: SparkSession) = {
    val df = DataSource.getDF(sparkSession, job, properties)
    val sink = new DataSink(job, properties, sparkSession)
    df.foreach(d => job.queries.map(q => sink.save(q, QueryExecutor.execute(q, d))))
  }

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("AuditProfiler").getOrCreate()
    val (jobs, props) = InputParser.get(args(0))
    jobs.map(j => run(j, props)(session))
    session.close()
  }
}

object InputJob {

  def getRawAuditJob(): Seq[AuditQueryJob] = {
    val sql = "select htable,count, selectcount, createcount,dropcount, NTILE(10) OVER (ORDER BY count) AS quartile from (select htable,count(*) as count, count(case when action = \"select\" then 1 end) as selectcount,count(case when action = \"create\" then 1 end) as createcount, count(case when action = \"drop\" then 1 end) as dropcount from hive group by htable)"
    val query = SqlQuery("accessrollups", sql, "hive")
    val job = AuditQueryJob(AggregationType.Daily, Seq(query), LocalDate.parse("20170628", DateTimeFormatter.ofPattern("yyyyMMdd")).atStartOfDay(), RangerAuditRaw("hiveServer2"))
    Seq(job)
  }

  def getAuditRollupJob(): Seq[AuditQueryJob] = {
    val sql = "select htable, selectcount from hive order by selectcount desc limit 5"
    val query = SqlQuery("selectcount", sql, "hive")
    val job = AuditQueryJob(AggregationType.Daily, Seq(query), LocalDate.parse("20170628", DateTimeFormatter.ofPattern("yyyyMMdd")).atStartOfDay(), AssetRollup("accessrollups"))
    Seq(job)
  }
}