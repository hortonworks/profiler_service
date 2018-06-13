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

import scala.collection.mutable.{Map => MutableMap}

object FrequentItems {

  /** A helper class wrapping `MutableMap[Any, Long]` for simplicity. */
  class FreqItemCounter(size: Int) extends Serializable {
    val baseMap: MutableMap[Any, Long] = MutableMap.empty[Any, Long]

    /**
      * Add a new example to the counts if it exists, otherwise deduct the count
      * from existing items.
      */
    def add(key: Any, count: Long): this.type = {
      if (baseMap.contains(key)) {
        baseMap(key) += count
      } else {
        if (baseMap.size < size) {
          baseMap += key -> count
        } else {
          val minCount = if (baseMap.values.isEmpty) 0 else baseMap.values.min
          val remainder = count - minCount
          if (remainder >= 0) {
            baseMap += key -> count // something will get kicked out, so we can add this
            baseMap.retain((k, v) => v > minCount)
            baseMap.transform((k, v) => v - minCount)
          } else {
            baseMap.transform((k, v) => v - count)
          }
        }
      }
      this
    }

    /**
      * Merge two maps of counts.
      *
      * @param other The map containing the co´unts for that partition
      */
    def merge(other: FreqItemCounter): this.type = {
      other.baseMap.foreach { case (k, v) =>
        add(k, v)
      }
      this
    }
  }

}

