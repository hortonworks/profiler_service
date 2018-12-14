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

package com.hortonworks.dataplane.profilers.tablestats.aggregators.frequentitems

import com.hortonworks.dataplane.profilers.tablestats.aggregators.{Aggregator, FrequentItemsCountState}
import com.hortonworks.dataplane.profilers.tablestats.utils.FrequentItemCountMetrics

import scala.collection.mutable.{Map => MutableMap}

class FrequentItemsCountAggregator(items: Set[Any])
  extends Aggregator[ItemCountAccumulator, Any] with Serializable {

  override def initialElement = new ItemCountAccumulator(items)

  override def mergeValue(acc: ItemCountAccumulator, elem: Any) = {
    if (elem != null) acc.add(elem, 1L)
    else acc
  }

  override def mergeCombiners(acc1: ItemCountAccumulator, acc2: ItemCountAccumulator) = acc1.merge(acc2.countMap)

  override def result(acc: ItemCountAccumulator) = {
    val sorted = acc.countMap.map{
      case(k,v) => (k,v)
    }.toSeq.sortBy(-_._2)
    new FrequentItemCountMetrics(sorted)
  }

  override def getState(acc: ItemCountAccumulator) = FrequentItemsCountState(acc)
}

class ItemCountAccumulator(items: Set[Any]) extends Serializable {
  val countMap: MutableMap[Any, Long] = MutableMap.empty[Any, Long]

  def add(key: Any, value: Long) = {
    if (items.contains(key)) {
      val newValue = countMap.getOrElse(key, 0L) + value
      countMap += key -> newValue
    }
    this
  }

  def merge(other: MutableMap[Any, Long]) = {
    other.foreach {
      case (k, v) =>
        add(k, v)
    }
    this
  }
}