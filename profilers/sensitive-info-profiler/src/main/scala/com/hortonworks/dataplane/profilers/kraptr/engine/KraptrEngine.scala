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

package com.hortonworks.dataplane.profilers.kraptr.engine

import com.hortonworks.dataplane.profilers.kraptr.common.KraptrContext
import com.hortonworks.dataplane.profilers.kraptr.dsl.TagCreator
import com.hortonworks.dataplane.profilers.kraptr.engine.loader.{BehaviourLoader, DSLLoader}
import com.hortonworks.dataplane.profilers.kraptr.engine.repl.DSLCompiler
import com.hortonworks.dataplane.profilers.kraptr.models.behaviour.BehaviourDefinition
import com.hortonworks.dataplane.profilers.kraptr.models.dsl.DSLandTags
import com.typesafe.scalalogging.LazyLogging

import scala.util.{Failure, Success, Try}

object KraptrEngine extends LazyLogging {

  def buildTagCreators(kraptrContext: KraptrContext): Try[List[TagCreator]] = {
    val possibleBehaviours = BehaviourLoader.loadBehaviours(kraptrContext).flatMap(
      checkForDuplicates[BehaviourDefinition](_, x => x.name, "Behaviours")
    )
    val possibleDsls: Try[List[DSLandTags]] = DSLLoader.loadDSLs(kraptrContext).map(_.filter(_.isEnabled))
    possibleBehaviours.flatMap(
      behaviours =>
        possibleDsls.flatMap(
          dsls => {
            DSLCompiler.compileAndCreateTagCreators(behaviours, dsls, kraptrContext)
          }
        )
    )
  }

  def checkForDuplicates[L](units: Seq[L], getId: L => String, unitName: String): Try[Seq[L]] = {
    val namesWithMoreThanOneValue = units.groupBy(getId).filter(_._2.length > 1).keys
    if (namesWithMoreThanOneValue.nonEmpty) {
      val errorMessage = s"$unitName : ${namesWithMoreThanOneValue.mkString(",")} have multiple definitions"
      logger.error(errorMessage)
      Failure(new Exception(errorMessage))
    }
    else {
      Success(units)
    }
  }


}
