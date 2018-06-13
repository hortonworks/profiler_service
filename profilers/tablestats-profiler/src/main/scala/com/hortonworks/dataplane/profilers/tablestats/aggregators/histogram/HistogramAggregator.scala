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

package com.hortonworks.dataplane.profilers.tablestats.aggregators.histogram

import com.hortonworks.dataplane.profilers.tablestats.aggregators.Aggregator
import com.hortonworks.dataplane.profilers.tablestats.utils.HistogramMetrics

class HistogramAggregator(bucketCount: Int, min: Double, max: Double) extends Aggregator[Array[Number], Number] with Serializable {

  private def customRange(min: Double, max: Double, steps: Int): IndexedSeq[Double] = {
    val span = max - min
    Range.Int(0, steps, 1).map(s => min + (s * span) / steps) :+ max
  }

  val range = if (min != max) {
    // Range.Double.inclusive(min, max, increment)
    // The above code doesn't always work. See Scala bug #SI-8782.
    // https://issues.scala-lang.org/browse/SI-8782
    customRange(min, max, bucketCount)
  } else {
    List(min, min)
  }

  val buckets = range.toArray
  val evenBuckets = true

  private def basicBucketFunction(e: Double): Option[Int] = {
    val location = java.util.Arrays.binarySearch(buckets, e)
    if (location < 0) {
      // If the location is less than 0 then the insertion point in the array
      // to keep it sorted is -location-1
      val insertionPoint = -location - 1
      // If we have to insert before the first element or after the last one
      // its out of bounds.
      // We do this rather than buckets.lengthCompare(insertionPoint)
      // because Array[Double] fails to override it (for now).
      if (insertionPoint > 0 && insertionPoint < buckets.length) {
        Some(insertionPoint - 1)
      } else {
        None
      }
    } else if (location < buckets.length - 1) {
      // Exact match, just insert here
      Some(location)
    } else {
      // Exact match to the last element
      Some(location - 1)
    }
  }

  // Determine the bucket function in constant time. Requires that buckets are evenly spaced
  private def fastBucketFunction(min: Double, max: Double, count: Int)(e: Double): Option[Int] = {
    // If our input is not a number unless the increment is also NaN then we fail fast
    if (e.isNaN || e < min || e > max) {
      None
    } else {
      // Compute ratio of e's distance along range to total range first, for better precision
      val bucketNumber = (((e - min) / (max - min)) * count).toInt
      // should be less than count, but will equal count if e == max, in which case
      // it's part of the last end-range-inclusive bucket, so return count-1
      Some(math.min(bucketNumber, count - 1))
    }
  }

  // Decide which bucket function to pass to histogramPartition. We decide here
  // rather than having a general function so that the decision need only be made
  // once rather than once per shard
  private val bucketFunction = if (evenBuckets) {
    fastBucketFunction(buckets.head, buckets.last, buckets.length - 1) _
  } else {
    basicBucketFunction _
  }

  override def initialElement = Array.fill[Number](buckets.length - 1)(0)

  override def mergeValue(acc: Array[Number], elem: Number): Array[Number] = {
    if (elem == null) return acc
    bucketFunction(elem.doubleValue()) match {
      case Some(x: Int) => acc(x) = acc(x).longValue() + 1
      case _ => {}
    }
    acc
  }

  override def mergeCombiners(acc1: Array[Number], acc2: Array[Number]) = {
    acc1.indices.foreach(i => acc1(i) = acc1(i).longValue() + acc2(i).longValue())
    acc1
  }

  override def result(acc: Array[Number]) = new HistogramMetrics(buckets, acc)
}
