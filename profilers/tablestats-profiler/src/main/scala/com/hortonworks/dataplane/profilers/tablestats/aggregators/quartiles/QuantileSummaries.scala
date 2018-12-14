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

package com.hortonworks.dataplane.profilers.tablestats.aggregators.quartiles

import com.hortonworks.dataplane.profilers.tablestats.aggregators.quartiles.QuantileSummaries.Stats

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * Helper class to compute approximate quantile summary.
  * This implementation is based on the algorithm proposed in the paper:
  * "Space-efficient Online Computation of Quantile Summaries" by Greenwald, Michael
  * and Khanna, Sanjeev. (http://dx.doi.org/10.1145/375663.375670)
  *
  * In order to optimize for speed, it maintains an internal buffer of the last seen samples,
  * and only inserts them after crossing a certain size threshold. This guarantees a near-constant
  * runtime complexity compared to the original algorithm.
  *
  * @param compressThreshold the compression threshold.
  *   After the internal buffer of statistics crosses this size, it attempts to compress the
  *   statistics together.
  * @param relativeError the target relative error.
  *   It is uniform across the complete range of values.
  * @param sampled a buffer of quantile statistics.
  *   See the G-K article for more details.
  * @param count the count of all the elements *inserted in the sampled buffer*
  *              (excluding the head buffer)
  */
case class QuantileSummaries(
                         val compressThreshold: Int,
                         val relativeError: Double,
                         val sampled: Array[Stats] = Array.empty,
                         val count: Long = 0L) extends Serializable {

  // a buffer of latest samples seen so far
  private val headSampled: ArrayBuffer[Double] = ArrayBuffer.empty

  import QuantileSummaries._

  def setHeadSampled(hs: ArrayBuffer[Double]) = headSampled ++= hs

  def getHeadSampled() = headSampled
  /**
    * Returns a summary with the given observation inserted into the summary.
    * This method may either modify in place the current summary (and return the same summary,
    * modified in place), or it may create a new summary from scratch it necessary.
    * @param x the new observation to insert into the summary
    */
  def insert(x: Double): QuantileSummaries = {
    headSampled += x
    if (headSampled.size >= defaultHeadSize) {
      val result = this.withHeadBufferInserted
      if (result.sampled.length >= compressThreshold) {
        result.compress()
      } else {
        result
      }
    } else {
      this
    }
  }

  /**
    * Inserts an array of (unsorted samples) in a batch, sorting the array first to traverse
    * the summary statistics in a single batch.
    *
    * This method does not modify the current object and returns if necessary a new copy.
    *
    * @return a new quantile summary object.
    */
  private def withHeadBufferInserted: QuantileSummaries = {
    if (headSampled.isEmpty) {
      return this
    }
    var currentCount = count
    val sorted = headSampled.toArray.sorted
    val newSamples: ArrayBuffer[Stats] = new ArrayBuffer[Stats]()
    // The index of the next element to insert
    var sampleIdx = 0
    // The index of the sample currently being inserted.
    var opsIdx: Int = 0
    while (opsIdx < sorted.length) {
      val currentSample = sorted(opsIdx)
      // Add all the samples before the next observation.
      while (sampleIdx < sampled.length && sampled(sampleIdx).value <= currentSample) {
        newSamples += sampled(sampleIdx)
        sampleIdx += 1
      }

      // If it is the first one to insert, of if it is the last one
      currentCount += 1
      val delta =
        if (newSamples.isEmpty || (sampleIdx == sampled.length && opsIdx == sorted.length - 1)) {
          0
        } else {
          math.floor(2 * relativeError * currentCount).toInt
        }

      val tuple = Stats(currentSample, 1, delta)
      newSamples += tuple
      opsIdx += 1
    }

    // Add all the remaining existing samples
    while (sampleIdx < sampled.length) {
      newSamples += sampled(sampleIdx)
      sampleIdx += 1
    }
    new QuantileSummaries(compressThreshold, relativeError, newSamples.toArray, currentCount)
  }

  /**
    * Returns a new summary that compresses the summary statistics and the head buffer.
    *
    * This implements the COMPRESS function of the GK algorithm. It does not modify the object.
    *
    * @return a new summary object with compressed statistics
    */
  def compress(): QuantileSummaries = {
    // Inserts all the elements first
    val inserted = this.withHeadBufferInserted
    assert(inserted.headSampled.isEmpty)
    assert(inserted.count == count + headSampled.size)
    val compressed =
      compressImmut(inserted.sampled, mergeThreshold = 2 * relativeError * inserted.count)
    new QuantileSummaries(compressThreshold, relativeError, compressed, inserted.count)
  }

  private def shallowCopy: QuantileSummaries = {
    new QuantileSummaries(compressThreshold, relativeError, sampled, count)
  }

  /**
    * Merges two (compressed) summaries together.
    *
    * Returns a new summary.
    */
  def merge(other: QuantileSummaries): QuantileSummaries = {
    require(headSampled.isEmpty, "Current buffer needs to be compressed before merge")
    require(other.headSampled.isEmpty, "Other buffer needs to be compressed before merge")
    if (other.count == 0) {
      this.shallowCopy
    } else if (count == 0) {
      other.shallowCopy
    } else {
      // Merge the two buffers.
      // The GK algorithm is a bit unclear about it, but it seems there is no need to adjust the
      // statistics during the merging: the invariants are still respected after the merge.
      // TODO: could replace full sort by ordered merge, the two lists are known to be sorted
      // already.
      val res = (sampled ++ other.sampled).sortBy(_.value)
      val comp = compressImmut(res, mergeThreshold = 2 * relativeError * count)
      new QuantileSummaries(
        other.compressThreshold, other.relativeError, comp, other.count + count)
    }
  }

  /**
    * Runs a query for a given quantile.
    * The result follows the approximation guarantees detailed above.
    * The query can only be run on a compressed summary: you need to call compress() before using
    * it.
    *
    * @param quantile the target quantile
    * @return
    */
  def query(quantile: Double): Option[Double] = {
    require(quantile >= 0 && quantile <= 1.0, "quantile should be in the range [0.0, 1.0]")
    require(headSampled.isEmpty,
      "Cannot operate on an uncompressed summary, call compress() first")

    if (sampled.isEmpty) return None

    if (quantile <= relativeError) {
      return Some(sampled.head.value)
    }

    if (quantile >= 1 - relativeError) {
      return Some(sampled.last.value)
    }

    // Target rank
    val rank = math.ceil(quantile * count).toInt
    val targetError = math.ceil(relativeError * count)
    // Minimum rank at current sample
    var minRank = 0
    var i = 1
    while (i < sampled.length - 1) {
      val curSample = sampled(i)
      minRank += curSample.g
      val maxRank = minRank + curSample.delta
      if (maxRank - targetError <= rank && rank <= minRank + targetError) {
        return Some(curSample.value)
      }
      i += 1
    }
    Some(sampled.last.value)
  }
}

object QuantileSummaries {
  // TODO(tjhunter) more tuning could be done one the constants here, but for now
  // the main cost of the algorithm is accessing the data in SQL.
  /**
    * The default value for the compression threshold.
    */
  val defaultCompressThreshold: Int = 10000

  /**
    * The size of the head buffer.
    */
  val defaultHeadSize: Int = 50000

  /**
    * The default value for the relative error (1%).
    * With this value, the best extreme percentiles that can be approximated are 1% and 99%.
    */
  val defaultRelativeError: Double = 0.01

  /**
    * Statistics from the Greenwald-Khanna paper.
    * @param value the sampled value
    * @param g the minimum rank jump from the previous value's minimum rank
    * @param delta the maximum span of the rank.
    */
  case class Stats(value: Double, g: Int, delta: Int)

  private def compressImmut(
                             currentSamples: IndexedSeq[Stats],
                             mergeThreshold: Double): Array[Stats] = {
    if (currentSamples.isEmpty) {
      return Array.empty[Stats]
    }
    val res = ListBuffer.empty[Stats]
    // Start for the last element, which is always part of the set.
    // The head contains the current new head, that may be merged with the current element.
    var head = currentSamples.last
    var i = currentSamples.size - 2
    // Do not compress the last element
    while (i >= 1) {
      // The current sample:
      val sample1 = currentSamples(i)
      // Do we need to compress?
      if (sample1.g + head.g + head.delta < mergeThreshold) {
        // Do not insert yet, just merge the current element into the head.
        head = head.copy(g = head.g + sample1.g)
      } else {
        // Prepend the current head, and keep the current sample as target for merging.
        res.prepend(head)
        head = sample1
      }
      i -= 1
    }
    res.prepend(head)
    // If necessary, add the minimum element:
    val currHead = currentSamples.head
    // don't add the minimum element if `currentSamples` has only one element (both `currHead` and
    // `head` point to the same element)
    if (currHead.value <= head.value && currentSamples.length > 1) {
      res.prepend(currentSamples.head)
    }
    res.toArray
  }
}
