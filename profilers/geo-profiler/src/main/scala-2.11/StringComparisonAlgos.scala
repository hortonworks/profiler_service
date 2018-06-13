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

/**
  * Created by visharma on 7/11/17.
  */

/**
  CREDITS : https://github.com/vkostyukov/scalacaster
  Directly copied from source code of github repo at
  https://github.com/vkostyukov/scalacaster/blob/master/src/primitive/Strings.scala
  since SBT dependency details are not available
  Have requested Author to suggest SBT dependency(if any)
  */

object StringComparisonAlgos {
  def matchRabinKarp(s: String, p: String): Int = {
    val n = s.length()
    val m = p.length()
    val q = 3355439
    val r = 256
    val d = (1 until m).foldLeft(1)((a, v) => (a * r) % q)

    def hash(ss: String, m: Int): Int =
      (0 until m).foldLeft(0)((a, v) => ((a * r) + ss.charAt(v)) % q)

    def loop(hs: Int, hp: Int, i: Int): Int =
      if (hs == hp) i - m
      else if (i == n) -1
      else {
        val hss = (hs - d * s.charAt(i - m) % q) % q
        loop((hss * r + s.charAt(i)) % q, hp, i + 1)
      }

    loop(hash(s, m), hash(p, m), m)
  }

  def longestCommonSubstring(a: String, b: String) : String = {
    def loop(m: Map[(Int, Int), Int], bestIndices: List[Int], i: Int, j: Int) : String = {
      if (i > a.length) {
        b.substring(bestIndices(1) - m((bestIndices(0),bestIndices(1))), bestIndices(1))
      } else if (i == 0 || j == 0) {
        loop(m + ((i,j) -> 0), bestIndices, if(j == b.length) i + 1 else i, if(j == b.length) 0 else j + 1)
      } else if (a(i-1) == b(j-1) && math.max(m((bestIndices(0),bestIndices(1))), m((i-1,j-1)) + 1) == (m((i-1,j-1)) + 1)) {
        loop(
          m + ((i,j) -> (m((i-1,j-1)) + 1)),
          List(i, j),
          if(j == b.length) i + 1 else i,
          if(j == b.length) 0 else j + 1
        )
      } else {
        loop(m + ((i,j) -> 0), bestIndices, if(j == b.length) i + 1 else i, if(j == b.length) 0 else j + 1)
      }
    }
    if(a == null || b == null){
      return ""
    }else {
      loop(Map[(Int, Int), Int](), List(0, 0), 0, 0)
    }
  }
}
