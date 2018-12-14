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

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus
import com.hortonworks.dataplane.profilers.tablestats.aggregators.frequentitems.FrequentItems.FreqItemCounter
import com.hortonworks.dataplane.profilers.tablestats.aggregators.frequentitems.ItemCountAccumulator
import com.hortonworks.dataplane.profilers.tablestats.aggregators.quartiles.QuantileSummaries
import com.hortonworks.dataplane.profilers.tablestats.utils._

import scala.collection.immutable.{SortedMap, TreeMap}
import scala.collection.mutable.ArrayBuffer

trait AggregateState extends Serializable {
  def merge(other: AggregateState): AggregateState

  def toMetrics: Metrics
}

case class NullState(count: Long) extends AggregateState {
  override def merge(other: AggregateState) = other match {
    case o: NullState => NullState(count + o.count)
    case _ => this
  }

  override def toMetrics = new NullCountMetrics(count)
}

case class StateWithColumnNameInfo(columnName: String, state: AggregateState) extends AggregateState {
  override def merge(other: AggregateState): StateWithColumnNameInfo = other match {
    case o: StateWithColumnNameInfo => StateWithColumnNameInfo(columnName, state.merge(o.state))
    case _ => this
  }

  override def toMetrics = new ColumnMetrics(columnName, state.toMetrics)
}

case class BooleanState(booleanAcc: BooleanAcc) extends AggregateState {
  override def merge(other: AggregateState) = other match {
    case o: BooleanState => BooleanState(booleanAcc.merge(o.booleanAcc))
    case _ => this
  }

  override def toMetrics = new BooleanMetrics(booleanAcc.trueCount, booleanAcc.falseCount, booleanAcc.otherCount)
}

case class CountState(count: Long) extends AggregateState {
  override def merge(other: AggregateState) = other match {
    case o: CountState => CountState(count + o.count)
    case _ => this
  }

  override def toMetrics = new CountMetrics(count)
}


case class DistinctState(hll: HyperLogLogPlus) extends AggregateState {
  override def merge(other: AggregateState) = other match {
    case o: DistinctState => {
      this.hll.addAll(o.hll)
      this
    }
    case _ => this
  }

  override def toMetrics = new DistinctMetrics(hll.cardinality())
}

case class StatsState(statsAccumulator: StatsAccumulator) extends AggregateState {
  override def merge(other: AggregateState) = other match {
    case o: StatsState => StatsState(statsAccumulator.merge(o.statsAccumulator))
    case _ => this
  }

  override def toMetrics = new StatsMetrics(statsAccumulator.result())
}

case class QuantileState(compressThreshold: Int,
                         relativeError: Double,
                         sampled: Array[QuantileSummaries.Stats] = Array.empty,
                         count: Long = 0L,
                         headSampled: ArrayBuffer[Double]) extends AggregateState {
  def getQuantileSummary() = {
    val qs: QuantileSummaries = new QuantileSummaries(compressThreshold, relativeError, sampled, count)
    qs.setHeadSampled(headSampled)
    qs
  }

  override def merge(other: AggregateState) = other match {
    case o: QuantileState => {
      val qs = this.getQuantileSummary()
      qs.merge(o.getQuantileSummary())
      this.copy(headSampled = qs.getHeadSampled())
    }
    case _ => this
  }

  override def toMetrics = {
    val quartilesArr = Array(0.0, 0.25, 0.50, 0.75, 0.99) //TODO its hardocded somewhere else, centralize it
    new QuartileMetrics(quartilesArr, quartilesArr.flatMap(getQuantileSummary.compress().query))
  }
}

case class HistogramState(acc: Array[Number]) extends AggregateState {
  override def merge(other: AggregateState) = other match {
    case o: HistogramState => {
      acc.indices.foreach(i => acc(i) = acc(i).longValue() + o.acc(i).longValue())
      HistogramState(acc)
    }
    case _ => this
  }

  override def toMetrics = ??? //TODO
}

case class FrequentItemsState(acc: FreqItemCounter) extends AggregateState {
  override def merge(other: AggregateState) = other match {
    case o: FrequentItemsState => FrequentItemsState(acc.merge(o.acc))
    case _ => this
  }

  override def toMetrics = {
    val topKeys = acc.baseMap.map(e => (e._1, e._2)).toSeq.sortBy(-_._2).take(20).map(_._1)
    new FrequentItemMetrics(topKeys.toList)
  }
}

case class FrequentItemsCountState(acc: ItemCountAccumulator) extends AggregateState {
  override def merge(other: AggregateState) = other match {
    case o: FrequentItemsCountState => FrequentItemsCountState(acc.merge(o.acc.countMap))
    case _ => this
  }

  override def toMetrics = {
    val sorted = acc.countMap.map {
      case (k, v) => (k, v)
    }.toSeq.sortBy(-_._2)
    new FrequentItemCountMetrics(sorted)
  }
}

case class StateSeq(states: Seq[StateWithColumnNameInfo]) extends AggregateState {
  override def merge(other: AggregateState) = other match {
    case o: StateSeq => {
      val merged: Seq[StateWithColumnNameInfo] = if (states.isEmpty || o.states.isEmpty) {
        if (states.isEmpty) o.states else states
      } else {
        states.zip(o.states).map {
          case (s1, s2) => s1.merge(s2)
        }
      }
      StateSeq(merged)
    }
    case _ => this
  }

  override def toMetrics = {
    val metricsSeq = states.map(_.toMetrics)
    new MetricsSeq(metricsSeq)
  }

  def toColumnCombinedStateMap: Map[String, ColumnCombinedAccumulator] = {
    val groupedState = states.groupBy(_.columnName)
    val resMap = groupedState.map {
      case (col, colStates) => var combState = ColumnCombinedAccumulator(col, None, None, None, None)
        colStates.map { colState =>
          colState.state match {
            case cs: NullState => combState = combState.copy(nullAcc = Option(cs.count))
            case cs: BooleanState => combState = combState.copy(booleanAcc = Option(cs.booleanAcc))
            case cs: StatsState => combState = combState.copy(statsAcc = Option(cs.statsAccumulator))
            case cs: CountState => combState = combState.copy(countAcc = Option(cs.count))
            case _ => combState
          }
        }
        (col, combState)
    }
    resMap
  }

}

case class ColumnCombinedAccumulator(columname: String, nullAcc: Option[Long], booleanAcc: Option[BooleanAcc], statsAcc: Option[StatsAccumulator], countAcc: Option[Long]) extends Serializable {

  val nullStatsAcc = new StatsAccumulator()

  def merge(other: ColumnCombinedAccumulator): ColumnCombinedAccumulator = {
    ColumnCombinedAccumulator(this.columname, Some(this.nullAcc.getOrElse(0L) + other.nullAcc.getOrElse(0L)),
      Some(booleanAcc.getOrElse(BooleanAcc(0, 0, 0)).merge(other.booleanAcc.getOrElse(BooleanAcc(0, 0, 0)))),
      Some(statsAcc.getOrElse(nullStatsAcc).merge(other.statsAcc.getOrElse(nullStatsAcc))), this.countAcc.map(v => (v + other.countAcc.getOrElse(0L))))
  }

  def toStates: Seq[StateWithColumnNameInfo] = {
    val stateSeq = Seq(StateWithColumnNameInfo(columname, NullState(nullAcc.getOrElse(0))),
      StateWithColumnNameInfo(columname, BooleanState(booleanAcc.getOrElse(BooleanAcc(0, 0, 0)))),
      StateWithColumnNameInfo(columname, StatsState(statsAcc.getOrElse(nullStatsAcc))))
    countAcc.map(v => (StateWithColumnNameInfo(columname, CountState(v)) +: stateSeq)).getOrElse(stateSeq)
  }
}

case class StateSeqPerPartition(partition: String, colCombStates: Map[String, ColumnCombinedAccumulator]) extends Serializable