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

import com.hortonworks.dataplane.profilers.auditprofiler.aggregate.{HdfsRangerAggregator, HiveRangerAggregator}
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract sealed class QueryExecutor {
  def execute(dataFrame: DataFrame, query: Query)(implicit session: SparkSession): DataFrame
}

object TopKQueryExecutor extends QueryExecutor {

  import org.apache.spark.sql.functions._

  override def execute(df: DataFrame, query: Query)(implicit session: SparkSession): DataFrame = {
    val topKQuery = query.asInstanceOf[TopKQuery]
    val filteredDf = topKQuery.filter.map(df.filter).getOrElse(df)
    val dataDf = filteredDf.groupBy(topKQuery.field).count().sort(desc("count")).limit(topKQuery.no)
    dataDf
  }
}

object SqlExecutor extends QueryExecutor {
  override def execute(dataFrame: DataFrame, query: Query)(implicit session: SparkSession): DataFrame = {
    val sqlQuery = query.asInstanceOf[SqlQuery]
    dataFrame.createOrReplaceTempView(sqlQuery.tablename)
    dataFrame.sqlContext.sql(sqlQuery.sql)
  }
}

object HdfsRangerAggregateExecutor extends QueryExecutor {
  override def execute(dataFrame: DataFrame, query: Query)(implicit session: SparkSession): DataFrame = {
    val sqlQuery = query.asInstanceOf[HdfsRangerAggregate]
    HdfsRangerAggregator.aggregateDF(dataFrame)
  }
}

object HiveRangerAggregateExecutor extends QueryExecutor {
  override def execute(dataFrame: DataFrame, query: Query)(implicit session: SparkSession): DataFrame = {
    val sqlQuery = query.asInstanceOf[HiveRangerAggregate]
    HiveRangerAggregator.aggregateDF(dataFrame)
  }
}

object QueryExecutor {

  def execute(query: Query, df: DataFrame)(implicit session: SparkSession): DataFrame = {
    query match {
      case q: TopKQuery => TopKQueryExecutor.execute(df, q)
      case q: SqlQuery => SqlExecutor.execute(df, q)
      case q: HdfsRangerAggregate => HdfsRangerAggregateExecutor.execute(df, q)
      case q: HiveRangerAggregate => HiveRangerAggregateExecutor.execute(df, q)
    }
  }

}

