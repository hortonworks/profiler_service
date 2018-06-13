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

package com.hortonworks.dataplane.profilers

import com.hortonworks.dataplane.profilers.label.{ColumnResult, LabelMatcher}
import org.apache.spark.sql.DataFrame

case class ColumnCount(matches : Seq[Int], nullCount : Int, total : Int)

class SensitiveLabelProfiler(labelMatchers: Seq[LabelMatcher[_]]) extends Serializable {

  def profile(df: DataFrame): Seq[ColumnResult] = {
    import df.sparkSession.implicits._
    val columns = df.columns
    val results: Seq[ColumnCount] = df.map {
      row =>
        row.toSeq.map {
          col =>
            val nullCount = if(col == null) 1 else 0

            val matches = labelMatchers
              .filter(labelMatcher => labelMatcher.isEnabled)
              .map(_.transformAndMatch(col)).map(b => if (b) 1 else 0)

            val total = 1
            ColumnCount(matches, nullCount, total)

        }
    }.reduce {
      (c1, c2) =>
        c1.zip(c2).map {
          case (x, y) =>
            val matches = x.matches.zip(y.matches).map {
            case (i, j) => i + j
          }
            val nullCount = x.nullCount + y.nullCount
            val total = x.total + y.total
            ColumnCount(matches, nullCount, total)
        }
    }

    columns.zip(results).flatMap {
      case (col, res) =>
        labelMatchers.filter(labelMatcher => labelMatcher.isEnabled).zip(res.matches).map {
          case (lm, count) => {
            ColumnResult(col, lm.name, count, res.nullCount, res.total)
          }
        }
    }
  }

  def getAllLabels(): Seq[String] = {
    labelMatchers.map(_.name)
  }

}