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

import com.rockymadden.stringmetric.similarity.DiceSorensenMetric
import com.rockymadden.stringmetric.similarity.JaccardMetric
import com.rockymadden.stringmetric.similarity.JaroMetric
import com.rockymadden.stringmetric.similarity.JaroWinklerMetric
import com.rockymadden.stringmetric.similarity.HammingMetric
import com.rockymadden.stringmetric.similarity.LevenshteinMetric
import com.rockymadden.stringmetric.similarity.NGramMetric
import com.rockymadden.stringmetric.similarity.OverlapMetric

object Test extends App{
//  println("City comparison scores")
//  println(JaroMetric.compare("city", "city_name").getOrElse(0))
//  println(JaroWinklerMetric.compare("city", "city_name").getOrElse(0))
//  println(JaccardMetric(1).compare("city", "city_name").getOrElse(0))
//  println(DiceSorensenMetric(1).compare("city", "city_name").getOrElse(0))
//  println(HammingMetric.compare("city", "city_name").getOrElse(0))
//  println(NGramMetric(1).compare("city", "city_name").getOrElse(0))
//  println(OverlapMetric(1).compare("city", "city_name").getOrElse(0))
//  println(OverlapMetric(1).compare("city", "ytic").getOrElse(0))
//  println("Latitiude comparison scores")
//  println(JaroMetric.compare("latitude", "lat").getOrElse(0))
//  println(JaroWinklerMetric.compare("latitude", "lat").getOrElse(0))
//  println(JaccardMetric(1).compare("latitude", "lat").getOrElse(0))
//  println(DiceSorensenMetric(1).compare("latitude", "lat").getOrElse(0))
//  println(HammingMetric.compare("latitude", "lat").getOrElse(0))
//  println(NGramMetric(1).compare("latitude", "lat").getOrElse(0))
//  println(OverlapMetric(1).compare("latitude", "lat").getOrElse(0))

  println(matchRabinKarp("aed_0&%d-scity_name", "city"))
  println(longestCommonSubstring("dfffdgcity_name", "qwqwcity_wnsd"))


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
    loop(Map[(Int, Int), Int](), List(0, 0), 0, 0)
  }
}