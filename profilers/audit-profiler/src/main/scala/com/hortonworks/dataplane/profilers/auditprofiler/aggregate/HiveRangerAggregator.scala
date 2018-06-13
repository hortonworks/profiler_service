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

package com.hortonworks.dataplane.profilers.auditprofiler.aggregate

import com.hortonworks.dataplane.profilers.auditprofiler.HiveDFTransformer
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

case class HiveAggregate(database: String, table: String, count:
Int, data: AggData, user: Map[String, AggData])

case class AggData(access: Map[String, Int], action: Map[String, Int], result: Map[Long, Int])

object HiveRangerAggregator {

  private def getAggData(rows: Iterable[Row]) = {
    val accessMap = rows.filter(_.getAs[String]("access") != null).groupBy(f => f.getAs[String]("access")).mapValues(_.size).toMap
    val actionMap = rows.filter(_.getAs[String]("action") != null).groupBy(f => f.getAs[String]("action")).mapValues(_.size).toMap
    val resultMap = rows.filter(_.getAs[String]("result") != null).groupBy(f => f.getAs[Long]("result")).mapValues(_.size).toMap
    AggData(accessMap, actionMap, resultMap)
  }

  def aggregateDF(df: DataFrame)(implicit session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._

    val dfRdd = df.rdd.groupBy {
      f =>
        val tableName = HiveDFTransformer.getTableNameRanger(f.getAs[String]("resource"), f.getAs[String]("resType"))
        if (tableName == null) None
        else {
          Some((tableName.split("\\.")(0), tableName.split("\\.")(1)))
        }
    }.filter(_._1.isDefined).mapValues {
      rows =>
        val data = getAggData(rows)
        val userMap = rows.groupBy(f => f.getAs[String]("reqUser")).mapValues(getAggData)
        (data, userMap, rows.size)
    }.map {
      f =>
        HiveAggregate(f._1.get._1, f._1.get._2, f._2._3, f._2._1, f._2._2)
    }

    dfRdd.toDF()
  }

}
