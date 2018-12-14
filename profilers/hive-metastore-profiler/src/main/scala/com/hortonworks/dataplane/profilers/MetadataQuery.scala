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
package com.hortonworks.dataplane.profilers

import com.hortonworks.dataplane.profilers.MetadataQuery.MetadataQuery
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object MetadataQuery extends Enumeration{
    type MetadataQuery = Value
    val TopSize, TopRows, TopPartitions,  InputFormat, OutputFormat, Db, CreateTime, Range = Value
}

object CannedCollectionQueries {
  def getQueryResults(session: SparkSession, df:DataFrame, query: MetadataQuery):Array[Row] = {
    import session.sqlContext.implicits._
    query match {
      case MetadataQuery.TopSize => df.select("table", "size").sort($"size".desc).take(10)
      case MetadataQuery.TopRows => df.select("table", "numRows").sort($"numRows".desc).take(10)
      case MetadataQuery.TopPartitions => df.select("table", "numPartitions").sort($"numPartitions".desc).take(10)
      case MetadataQuery.Db => df.select("db").groupBy("db").count().sort($"count".desc).take(10)
      case MetadataQuery.InputFormat => df.select("inputFormat").groupBy("inputFormat").count().sort($"count".desc).take(10)
      case MetadataQuery.OutputFormat => df.select("outputFormat").groupBy("outputFormat").count().sort($"count".desc).take(10)
      case MetadataQuery.CreateTime => df.select("table", "createTime").sort($"createTime".desc).take(10)
      case MetadataQuery.Range => df.groupBy("db").agg(("size", "max"), ("size", "min"), ("numRows", "max"),
        ("numRows", "min")).collect()
    }
  }
}
