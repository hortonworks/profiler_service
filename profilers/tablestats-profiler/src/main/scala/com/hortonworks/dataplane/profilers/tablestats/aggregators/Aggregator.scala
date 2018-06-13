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

package com.hortonworks.dataplane.profilers.tablestats.aggregators

import com.hortonworks.dataplane.profilers.tablestats.aggregators.Aggregator.RowInputAggregator
import com.hortonworks.dataplane.profilers.tablestats.utils.{ColumnMetrics, Metrics}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.reflect.ClassTag

trait Aggregator[C, IN] {

  self: Serializable =>

  def execute(rdd: RDD[IN])(implicit clazz: ClassTag[C]): Metrics = {
    result(rdd.treeAggregate(initialElement)(mergeValue, mergeCombiners))
  }

  def initialElement: C

  def mergeValue(acc: C, elem: IN): C

  def mergeCombiners(acc1: C, acc2: C): C

  def mapInputRow(colName: String)
                 (f: Row => IN = (row) => row.getAs[IN](colName)): Aggregator[C, Row] =
    RowInputAggregator(this, colName, f)

  def result(acc: C): Metrics

}

object Aggregator {

  private[Aggregator] case class RowInputAggregator[C, IN](aggregator: Aggregator[C, IN],
                                                           colName: String, f: Row => IN)
    extends Aggregator[C, Row] {

    override def initialElement: C = aggregator.initialElement

    override def mergeValue(acc: C, elem: Row): C = aggregator.mergeValue(acc, f(elem))

    override def mergeCombiners(acc1: C, acc2: C): C = aggregator.mergeCombiners(acc1, acc2)

    override def result(acc: C): Metrics = new ColumnMetrics(colName, aggregator.result(acc))

  }

}
