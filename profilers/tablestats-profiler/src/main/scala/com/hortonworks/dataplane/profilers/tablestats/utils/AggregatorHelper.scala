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

import com.hortonworks.dataplane.profilers.tablestats.aggregators.{Aggregator, CountAggregator, MultiAggregator}
import com.hortonworks.dataplane.profilers.tablestats.utils.DataType.DataType
import com.hortonworks.dataplane.profilers.tablestats.utils.aggregator.DistinctAggregatorProvider.FrequentItemsCountAggregatorProvider
import com.hortonworks.dataplane.profilers.tablestats.utils.aggregator._
import org.apache.spark.sql.{DataFrame, Row}

object AggregatorHelper {

  def getColumns(df: DataFrame, dataType: DataType) = {
    df.schema.fields
      .filter(f => DataType.getDataType(f.dataType.toString) == dataType)
      .map(_.name)
  }

  def getColumnsWithType(df: DataFrame) = {
    df.schema.fields
      .map(f => (f.name, f.dataType))
  }

  def allColumns(df: DataFrame) = df.schema.fields.map(_.name)

  val aggregatorProviders = Seq(
    NullAggregatorProvider,
    BooleanAggregatorProvider,
    QuartileAggregatorProvider,
    HistogramAggregatorProvider,
    FrequentItemsAggregatorProvider,
    DistinctAggregatorProvider
  )

  def getAggregators(df: DataFrame, stats: Array[Row]) = {
    val aggregators = aggregatorProviders.map(_.get(df, stats)).flatten
    val countAggregator = new CountAggregator()
    new MultiAggregator((aggregators ++ Seq(countAggregator)).map(_.asInstanceOf[Aggregator[Any, Row]]))
  }

  def getFrequentCountAggregators(df: DataFrame, metrics: Seq[Metrics]) = {
    new MultiAggregator(FrequentItemsCountAggregatorProvider.get(df, metrics))
  }

}
