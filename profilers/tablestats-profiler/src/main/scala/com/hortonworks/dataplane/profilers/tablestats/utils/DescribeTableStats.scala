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

package com.hortonworks.dataplane.profilers.tablestats.utils

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, Row}

import scala.util.{Failure, Success, Try}

class DescribeTableStats(df: DataFrame) extends Serializable with LazyLogging {

  private val stringLengthColSuffix = "_length"
  private var stats: Array[Row] = null

  val stringCols = AggregatorHelper.getColumns(df, DataType.String)
  val numericCols = AggregatorHelper.getColumns(df, DataType.Numeric)
  val colsWithTypes = AggregatorHelper.getColumnsWithType(df)

  val sparkToHiveDataTypeMap = Map(("StringType" -> "string"), ("BooleanType" -> "boolean"),
    ("DoubleType" -> "double"), ("DecimalType" -> "decimal"), ("FloatType" -> "float"),
    ("LongType" -> "long"), ("IntegerType" -> "int"))

  def compute() = {

    import org.apache.spark.sql.functions._

    def getStringLength(value: String): Int = {
      if (value == null) 0
      else value.size
    }

    val stringWithLengthCols = stringCols.map(_ + stringLengthColSuffix)


    val stringAndNumericCols = stringWithLengthCols ++ numericCols

    val stats = stringCols.foldLeft(df)((f, v) => {
      val tableColumn = udf(getStringLength _).apply(col(v))
      val colName = v + stringLengthColSuffix
      f.withColumn(colName, tableColumn)
    }).describe(stringAndNumericCols: _*).collect()

    this.stats = stats
    stats
  }


  def getMetrics() = {
    if (stats == null) throw new IllegalStateException("Stats not computed")

    def getStats(colName: String): Seq[DoubleMetrics] = {
      Try {
        val count = stats(0).getAs[String](colName).toDouble
        val mean = stats(1).getAs[String](colName).toDouble
        val stddev = stats(2).getAs[String](colName).toDouble
        val min = stats(3).getAs[String](colName).toDouble
        val max = stats(4).getAs[String](colName).toDouble

        Seq(
          new DoubleMetrics("count", count),
          new DoubleMetrics("mean", mean),
          new DoubleMetrics("stddev", stddev),
          new DoubleMetrics("min", min),
          new DoubleMetrics("max", max)
        )
      } match {
        case Success(seq) => seq
        case Failure(e) =>
          logger.info(s"stats metrics : ${stats.mkString(",")}")
          logger.warn("Error while getting stats", e)
          Seq()
      }

    }

    val numericStats = numericCols.map { col =>
      getStats(col).map(new ColumnMetrics(col, _))
    }.flatten

    val stringStats = stringCols.map { col =>
      getStats(col + stringLengthColSuffix).map(new ColumnMetrics(col, _))
    }.flatten


    new MetricsSeq(numericStats ++ stringStats)
  }

  def getTypes() = {
    val columnTypes = colsWithTypes.map(col => new ColumnMetrics(col._1,
      new ColumnTypeMetrics(sparkToHiveDataTypeMap.getOrElse(col._2.toString, col._2.toString))))
    new MetricsSeq(columnTypes)
  }
}
