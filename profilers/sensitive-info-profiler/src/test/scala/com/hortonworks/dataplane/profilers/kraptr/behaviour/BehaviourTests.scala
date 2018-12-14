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

package com.hortonworks.dataplane.profilers.kraptr.behaviour

import com.hortonworks.dataplane.profilers.kraptr.common.KraptrContext
import com.hortonworks.dataplane.profilers.kraptr.engine.repl.DSLCompiler
import com.hortonworks.dataplane.profilers.kraptr.models.dsl.{DSLandTags, MatchType}
import com.hortonworks.dataplane.profilers.kraptr.vfs.hdfs.HDFSFileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import org.scalatest.{AsyncFlatSpec, Matchers}

class BehaviourTests extends AsyncFlatSpec with Matchers {

  private val resourcePath: String = getClass.getResource("/dsl/").getPath

  implicit val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()

  private val profilerInstanceName = "sensitiveinfo"
  val context = KraptrContext(resourcePath, profilerInstanceName,
    HDFSFileSystem(FileSystem.get(new Configuration())), spark, None)


  "luhn_check" should "be able to correctly identify valid strings" in {
    val tags = DSLandTags(MatchType.name, "luhn_check", List("tag1"), 100)
    val triedCreators = DSLCompiler.compileAndCreateTagCreators(List(), List(tags), context)
    assert(triedCreators.get.head.decider.process("49927398716"))
    assert(!triedCreators.get.head.decider.process("49927398717"))
    assert(triedCreators.get.head.decider.process("3304611894"))
    assert(!triedCreators.get.head.decider.process("1234567812345678"))
  }

  "iban_check" should "be able to correctly identify valid strings" in {
    val tags = DSLandTags(MatchType.name, "iban_check", List("tag1"), 100)
    val triedCreators = DSLCompiler.compileAndCreateTagCreators(List(), List(tags), context)
    assert(triedCreators.get.head.decider.process("BG18RZBB91550123456789"))
    assert(!triedCreators.get.head.decider.process("FO9264600123456785"))
    assert(triedCreators.get.head.decider.process("HU93116000060000000012345676"))
    assert(!triedCreators.get.head.decider.process("IT60X0542811101000000123455"))
  }

}
