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

import com.hortonworks.dataplane.profilers.tablestats.utils.{AggregatorHelper, CountMetrics, DescribeTableStats, MetricsSeq}
import org.apache.spark.sql.{DataFrame, SparkSession}

object TableStats {

  def getTableDF(sparkSession: SparkSession, database: String, table: String, samplePercent: Double) = {

    val hive_query = "select * from " + database + "." + table
    println("Hive Query : " + hive_query)

    val tableDF = sparkSession.sql(hive_query)
    val sampleFraction = samplePercent.toDouble / 100.0


    val sampledDF = if (samplePercent == 100) tableDF
    else tableDF.sample(false, sampleFraction)

    sampledDF
  }

  def profileDF(tableDF: DataFrame) = {
    val ds = new DescribeTableStats(tableDF)
    val stats = ds.compute()
    stats.foreach(println)

    val metrics: MetricsSeq = AggregatorHelper.getAggregators(tableDF, stats)
      .execute(tableDF.rdd).asInstanceOf[MetricsSeq]

    val frequentCountMetrics = AggregatorHelper.getFrequentCountAggregators(tableDF, metrics.seq)
      .execute(tableDF.rdd).asInstanceOf[MetricsSeq]

    val statsMetrics: MetricsSeq = ds.getMetrics()
    val typeNames: MetricsSeq = ds.getTypes()
    new MetricsSeq(statsMetrics.seq ++ metrics.seq ++ typeNames.seq ++ frequentCountMetrics.seq)
  }

  def rowCount(metrics: MetricsSeq) = {
    metrics.seq
      .filter(_.isInstanceOf[CountMetrics])
      .map(_.asInstanceOf[CountMetrics])
      .head.count
  }
}
