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

import com.hortonworks.dataplane.profilers.tablestats.aggregators.Stats
import org.json4s.JValue
import org.json4s.JsonAST.JArray
import org.json4s.DefaultReaders._
import org.json4s._
import org.json4s.jackson.JsonMethods._

trait Metrics {
  def toJson: JValue
}

class MetricsSeq(val seq: Seq[ColumnMetrics]) extends Metrics {
  override def toJson = JArray(seq.map(_.toJson).toList)
}

class ColumnMetrics(val colName: String, val metrics: Metrics) extends Metrics {
  override def toJson = JObject(JField(colName, metrics.toJson))
}

class ColumnTypeMetrics(value: String) extends Metrics {
  override def toJson = JObject(JField("type", JString(value)))
}

class DistinctMetrics(no: Long) extends Metrics {
  override def toJson = JObject(JField("distinct", JInt(no)))
}

class FrequentItemMetrics(val items: Seq[Any]) extends Metrics {
  override def toJson = JObject(JField("fq", JString(items.mkString("[", ",", "]"))))
}

class FrequentItemCountMetrics(val items: Seq[(Any, Long)]) extends Metrics {
  val stringValue = items.map {
    case (p, q) =>
      "{\"bin\":\"" + p.toString + "\"," + "\"count\":" + q.toString + "}"
  }.toList.mkString("[", ",", "]")

  override def toJson = JObject(JField("fqh", JString(stringValue)))

}

class NullCountMetrics(count: Long) extends Metrics {
  override def toJson = JObject(JField("null", JInt(count)))
}

class DoubleMetrics(field: String, value: Double) extends Metrics {
  override def toJson = JObject(JField(field, JDouble(value)))
}

class BooleanMetrics(trueCount: Long, falseCount: Long, otherCount: Long) extends Metrics {
  override def toJson = JObject(
    JField("trueCount", JInt(trueCount)),
    JField("falseCount", JInt(falseCount)),
    JField("otherCount", JInt(otherCount))
  )
}

class QuartileMetrics(probabilities: Array[Double], quartiles: Array[Double]) extends Metrics {
  override def toJson = {
    val q = probabilities.zip(quartiles).map {
      case (p, q) =>
        "{\"quartile\":" + p.toString + "," + "\"value\":" + q.toString + "}"
    }.toList.mkString("[", ",", "]")
    JObject(JField("quartiles", JString(q)))
  }
}

class HistogramMetrics(buckets: Array[Double], histogram: Array[Number]) extends Metrics {
  override def toJson: JValue = {
    val q = buckets.zip(histogram).map {
      case (p, q) =>
        "{\"bin\":" + p.toString + "," + "\"count\":" + q.toString + "}"
    }.toList.mkString("[", ",", "]")
    JObject(JField("histogram", JString(q)))
  }
}

class StatsMetrics(val stats: Stats) extends Metrics {
  override def toJson = JObject(
    JField("count", JDouble(stats.count)),
    JField("mean", JDouble(stats.mean)),
    JField("stddev", JDouble(stats.stdDev)),
    JField("min", JDouble(stats.min)),
    JField("max", JDouble(stats.max))
  )
}

class CountMetrics(val count: Long) extends Metrics {
  override def toJson: JValue = JObject(JField("rowCount", JInt(count)))
}

case class TableMetrics(rowCount:Option[Long], samplePercent:String, sampleTime:String)
