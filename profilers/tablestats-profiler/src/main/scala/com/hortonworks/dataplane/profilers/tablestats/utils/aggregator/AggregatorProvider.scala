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

package com.hortonworks.dataplane.profilers.tablestats.utils.aggregator

import com.hortonworks.dataplane.profilers.tablestats.TableStatsProfiler._
import com.hortonworks.dataplane.profilers.tablestats.aggregators.frequentitems.{FrequentItemsAggregator, FrequentItemsCountAggregator}
import com.hortonworks.dataplane.profilers.tablestats.aggregators.histogram.HistogramAggregator
import com.hortonworks.dataplane.profilers.tablestats.aggregators.hll.DistinctAggregator
import com.hortonworks.dataplane.profilers.tablestats.aggregators.{Aggregator, BooleanAggregator, CountAggregator, StatsAggregator}
import com.hortonworks.dataplane.profilers.tablestats.aggregators.nullaggregator.NullAggregator
import com.hortonworks.dataplane.profilers.tablestats.aggregators.quartiles.QuartileAggregator
import com.hortonworks.dataplane.profilers.tablestats.utils._
import org.apache.spark.sql.{DataFrame, Row}

trait AggregatorProvider {
  def get(df: DataFrame, metrics: Seq[Metrics]): Seq[Aggregator[_, Row]]
}


object NullAggregatorProvider extends AggregatorProvider {
  override def get(df: DataFrame, metrics: Seq[Metrics] = Nil): Seq[Aggregator[Long, Row]] = {

    val columns = AggregatorHelper.allColumns(df)
    columns.map(new NullAggregator().mapInputRow(_)()).toSeq
  }
}

object CountAggregratorProvider extends AggregatorProvider {
  override def get(df: DataFrame, metrics: Seq[Metrics] = Nil): Seq[Aggregator[Long, Row]] = {

    val columns = Array(AggregatorHelper.firstColumn(df))
    columns.map(new CountAggregator().mapInputRow(_)()).toSeq
  }
}

object BooleanAggregatorProvider extends AggregatorProvider {
  override def get(df: DataFrame, metrics: Seq[Metrics] = Nil) = {

    val booleanCols = AggregatorHelper.getColumns(df, DataType.Boolean)
    booleanCols.map(new BooleanAggregator().mapInputRow(_)()).toSeq
  }
}

object QuartileAggregatorProvider extends AggregatorProvider {
  override def get(df: DataFrame, metrics: Seq[Metrics] = Nil) = {

    val numericCols = AggregatorHelper.getColumns(df, DataType.Numeric)
    val quartilesArr = Array(0.0, 0.25, 0.50, 0.75, 0.99)
    numericCols.map(new QuartileAggregator(quartilesArr, 0.05).mapInputRow(_)()).toSeq
  }
}

object HistogramAggregatorProvider extends AggregatorProvider {
  override def get(df: DataFrame, metrics: Seq[Metrics] = Nil) = {
    val numericCols = AggregatorHelper.getColumns(df, DataType.Numeric)

    val aggs = numericCols.map { c =>
      val metricOpt = metrics.filter {
        m =>
          m.isInstanceOf[ColumnMetrics] &&
            m.asInstanceOf[ColumnMetrics].colName == c &&
            m.asInstanceOf[ColumnMetrics].metrics.isInstanceOf[StatsMetrics]
      }.map(_.asInstanceOf[ColumnMetrics].metrics.asInstanceOf[StatsMetrics].stats)
        .headOption

      metricOpt.map {
        m =>
          new HistogramAggregator(10, m.min, m.max).mapInputRow(c)()
      }
    }.filter(_.isDefined).map(_.get).toSeq

    aggs
  }
}

object FrequentItemsAggregatorProvider extends AggregatorProvider {

  override def get(df: DataFrame, metrics: Seq[Metrics] = Nil) = {
    val columns = AggregatorHelper.allColumns(df)
    columns.map(
      c => new FrequentItemsAggregator(0.005).mapInputRow(c)()
    ).toSeq
  }
}

object StatsAggregatorProvider extends AggregatorProvider {

  def getStringLength(columnName: String)(row: Row): Number = {
    val value = row.getAs[String](columnName)
    if (value == null) null
    else value.length
  }

  override def get(df: DataFrame, metrics: Seq[Metrics] = Nil) = {

    val numericCols = AggregatorHelper.getColumns(df, DataType.Numeric)
    val numericAggs = numericCols.map(new StatsAggregator().mapInputRow(_)()).toSeq

    val stringCols = AggregatorHelper.getColumns(df, DataType.String)
    val stringAggs = stringCols.map {
      colName =>
        new StatsAggregator().mapInputRow(colName)(getStringLength(colName))
    }.toSeq

    numericAggs ++ stringAggs
  }
}

object DistinctAggregatorProvider extends AggregatorProvider {

  override def get(df: DataFrame, metrics: Seq[Metrics] = Nil) = {
    val columns = AggregatorHelper.allColumns(df)
    columns.map {
      c =>
        new DistinctAggregator().mapInputRow(c)()
    }.toSeq
  }

  /**
    * This depends on frequent Item Aggregator output
    * Not sub class of Aggreagte Provider
    * 3rd pass
    */
  object FrequentItemsCountAggregatorProvider extends AggregatorProvider {

    def get(df: DataFrame, metrics: Seq[Metrics]) = {
      val columns = AggregatorHelper.allColumns(df)

      val aggs = columns.map { c =>
        val metricOpt = metrics.filter {
          m =>
            m.isInstanceOf[ColumnMetrics] &&
              m.asInstanceOf[ColumnMetrics].colName == c &&
              m.asInstanceOf[ColumnMetrics].metrics.isInstanceOf[FrequentItemMetrics]
        }.map(_.asInstanceOf[ColumnMetrics].metrics.asInstanceOf[FrequentItemMetrics].items)
          .headOption

        metricOpt.map {
          m =>
            new FrequentItemsCountAggregator(m.toSet).mapInputRow(c)()
        }
      }.filter(_.isDefined).map(_.get).toSeq

      aggs
    }
  }

}

