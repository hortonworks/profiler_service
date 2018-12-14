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

package com.hortonworks.dataplane.profilers.kraptr.engine.repl


import com.hortonworks.dataplane.profilers.kraptr.common.KraptrContext
import com.hortonworks.dataplane.profilers.kraptr.engine.KraptrEngine
import com.hortonworks.dataplane.profilers.kraptr.models.behaviour.{BehaviourConfigurationType, BehaviourDefinition}
import com.hortonworks.dataplane.profilers.kraptr.models.dsl.{DSLandTags, MatchType}
import com.hortonworks.dataplane.profilers.kraptr.vfs.hdfs.HDFSFileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import org.scalatest.{AsyncFlatSpec, Matchers}

class DSLCompilerTest extends AsyncFlatSpec with Matchers {

  private val resourcePath: String = getClass.getResource("/dsl/").getPath

  val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()

  private val profilerInstanceName = "sensitivityinstance"
  val context = KraptrContext(resourcePath, profilerInstanceName,
    HDFSFileSystem(FileSystem.get(new Configuration())), spark, None, "kraptr1")


  "Kraptr Engine" should "be able to load a behaviour with no configuration requirements" in {

    val behaviourIdentifier = "behaviour1"
    val behaviour = BehaviourDefinition(behaviourIdentifier, "token matches \"hello\"", BehaviourConfigurationType.none)
    val tags = DSLandTags(MatchType.name, behaviourIdentifier, List("tag1"), 100)
    val triedCreators = DSLCompiler.compileAndCreateTagCreators(List(behaviour), List(tags), context)
    assert(triedCreators.get.head.apply("hello!").isEmpty)
    assert(triedCreators.get.head.apply("hello").contains("tag1"))
  }


  "Kraptr Engine" should "be able to load a behaviour with list type configuration requirements" in {

    val behaviourIdentifier = "behaviour2"
    val behaviour = BehaviourDefinition(behaviourIdentifier, "token matches configs.head", BehaviourConfigurationType.list)
    val tags = DSLandTags(MatchType.name, "behaviour2(\"hello\")", List("tag1"), 100)
    val triedCreators = DSLCompiler.compileAndCreateTagCreators(List(behaviour), List(tags), context)
    assert(triedCreators.get.head.apply("hello!").isEmpty)
    assert(triedCreators.get.head.apply("hello").contains("tag1"))
  }


  "Kraptr Engine" should "be able to load a behaviour with map type configuration requirements" in {

    val behaviourIdentifier = "behaviour3"
    val behaviour = BehaviourDefinition(behaviourIdentifier, "token matches configs(\"checkFor\")", BehaviourConfigurationType.map)
    val tags = DSLandTags(MatchType.name, "behaviour3(\"checkFor\"-->\"hello\")", List("tag1"), 100)
    val triedCreators = DSLCompiler.compileAndCreateTagCreators(List(behaviour), List(tags), context)
    assert(triedCreators.get.head.apply("hello!").isEmpty)
    assert(triedCreators.get.head.apply("hello").contains("tag1"))
  }

  "Kraptr Engine" should "be able to load behaviours,tags and dsls from files" in {
    val triedTuple = KraptrEngine.buildTagCreators(context)
    assert(triedTuple.isSuccess &&
      triedTuple.get.length == 3)
  }


  "Kraptr Engine" should "be able to apply `and` on two behaviours" in {
    val behaviour1 = BehaviourDefinition("behaviour4", "token == \"hello\"", BehaviourConfigurationType.none)
    val behaviour2 = BehaviourDefinition("behaviour5", "token.size==5", BehaviourConfigurationType.none)
    val behaviour3 = BehaviourDefinition("behaviour6", "token.size==6", BehaviourConfigurationType.none)
    val tags1 = DSLandTags(MatchType.name, "behaviour4 and behaviour5", List("tag1"), 100)
    val tags2 = DSLandTags(MatchType.name, "behaviour4 and behaviour6", List("tag2"), 100)
    val triedCreators = DSLCompiler.compileAndCreateTagCreators(List(behaviour1, behaviour2, behaviour3), List(tags1, tags2), context)
    val tags = triedCreators.get.flatMap(_.apply("hello"))
    assert(tags.size == 1)
    assert(tags.head == "tag1")
  }

  "Kraptr Engine" should "be able to apply `or`  on two behaviours" in {
    val behaviour1 = BehaviourDefinition("behaviour7", "token == \"hello\"", BehaviourConfigurationType.none)
    val behaviour3 = BehaviourDefinition("behaviour8", "token.size==6", BehaviourConfigurationType.none)
    val tags2 = DSLandTags(MatchType.name, "behaviour7 or behaviour8", List("tag1"), 100)
    val triedCreators = DSLCompiler.compileAndCreateTagCreators(List(behaviour1, behaviour3), List(tags2), context)
    assert(triedCreators.get.head.apply("hello!").contains("tag1"))
  }

  "Kraptr Engine" should "be able to apply `not`  on a behaviour" in {
    val behaviour1 = BehaviourDefinition("behaviour9", "token == \"hello\"", BehaviourConfigurationType.none)
    val behaviour3 = BehaviourDefinition("behaviour10", "token.size==6", BehaviourConfigurationType.none)
    val tags2 = DSLandTags(MatchType.name, "not(behaviour9) and behaviour10", List("tag1"), 100)
    val triedCreators = DSLCompiler.compileAndCreateTagCreators(List(behaviour1, behaviour3), List(tags2), context)
    assert(triedCreators.get.head.apply("hello!").contains("tag1"))
  }

  "Kraptr Engine" should "be able to apply `all` and `any` on a behaviour" in {
    val behaviour1 = BehaviourDefinition("behaviour11", "token == \"hello\"", BehaviourConfigurationType.none)
    val behaviour2 = BehaviourDefinition("behaviour12", "token.size==6", BehaviourConfigurationType.none)
    val behaviour3 = BehaviourDefinition("behaviour13", "token.size==5", BehaviourConfigurationType.none)
    val tags1 = DSLandTags(MatchType.name, "all(behaviour11,behaviour12,behaviour13)", List("tag1"), 100)
    val tags2 = DSLandTags(MatchType.name, "any(behaviour11,behaviour12,behaviour13)", List("tag2"), 100)
    val triedCreators = DSLCompiler.compileAndCreateTagCreators(List(behaviour1, behaviour2, behaviour3),
      List(tags1, tags2), context)
    val tags = triedCreators.get.flatMap(_.apply("hello"))
    assert(tags.size == 1)
    assert(tags.head == "tag2")
  }


  "Kraptr Engine" should "be able to apply `give $ choose $ otherwise $` on behaviours" in {
    val behaviour1 = BehaviourDefinition("behaviour14", "token == \"hello\"", BehaviourConfigurationType.none)
    val behaviour3 = BehaviourDefinition("behaviour16", "token.size==5", BehaviourConfigurationType.none)
    val behaviour4 = BehaviourDefinition("behaviour17", "token.size==6", BehaviourConfigurationType.none)
    val tags1 = DSLandTags(MatchType.name, "given(behaviour14) choose behaviour16", List("tag1"), 100)
    val tags2 = DSLandTags(MatchType.name, "given(behaviour14) choose behaviour16 otherwise behaviour17", List("tag2"), 100)
    val behaviours = List(behaviour1, behaviour3, behaviour4)
    val givenChoose = DSLCompiler.compileAndCreateTagCreators(behaviours,
      List(tags1), context).get.head.apply("hello")
    assert(givenChoose.size == 1)
    assert(givenChoose.contains("tag1"))
    val givenChooseOtherwise = DSLCompiler.compileAndCreateTagCreators(behaviours,
      List(tags2), context).get.head.apply("hello!")
    assert(givenChooseOtherwise.size == 1)
    assert(givenChooseOtherwise.contains("tag2"))
  }

  "Kraptr Engine" should "not allow arbitrary code execution" in {
    val behaviour1 = BehaviourDefinition("behaviour18", "token == \"hello\"", BehaviourConfigurationType.none)
    val badDsl = DSLandTags(MatchType.name, "System.currentTimeMillis()" +
      "\nbehaviour18", List("tag1"), 100)
    assert(DSLCompiler.compileAndCreateTagCreators(List(behaviour1),
      List(badDsl), context).isFailure)
  }

}
