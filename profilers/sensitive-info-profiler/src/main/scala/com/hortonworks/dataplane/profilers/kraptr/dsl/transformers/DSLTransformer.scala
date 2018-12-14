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

package com.hortonworks.dataplane.profilers.kraptr.dsl.transformers

import com.hortonworks.dataplane.profilers.kraptr.behaviour.inbuilt.InbuiltBehaviours
import com.hortonworks.dataplane.profilers.kraptr.behaviour.special.given.{Given, GivenElse}
import com.hortonworks.dataplane.profilers.kraptr.behaviour.{Decider, SpecialOperations}
import com.hortonworks.dataplane.profilers.kraptr.common.CustomTypes.CompilableCode
import com.hortonworks.dataplane.profilers.kraptr.dsl.Implicits
import com.hortonworks.dataplane.profilers.kraptr.dsl.restriction.KraptrString
import com.hortonworks.dataplane.profilers.kraptr.engine.repl.DSLCompiler
import com.hortonworks.dataplane.profilers.kraptr.models.dsl.DSLandTags
import com.typesafe.scalalogging.LazyLogging

import scala.util.{Success, Try}

object DSLTransformer extends LazyLogging {
  def toCompilableCode(dslAndTags: DSLandTags): Try[CompilableCode] = {
    val verifiedKraptrStringName = classOf[KraptrString].getCanonicalName
    val verifiedDeciderName = classOf[Decider].getCanonicalName
    val verifiedSpecialOperationsName = classOf[SpecialOperations].getCanonicalName
    val verifiedInbuiltBehavioursName = classOf[InbuiltBehaviours].getCanonicalName
    val verifiedGivenName = classOf[Given].getCanonicalName
    val verifiedGivenElseName = classOf[GivenElse].getCanonicalName
    val verifiedImplicitsName = classOf[Implicits].getCanonicalName
    val allowedClasses = List(verifiedDeciderName, verifiedSpecialOperationsName,
      verifiedInbuiltBehavioursName, verifiedGivenName, verifiedGivenElseName,
      verifiedImplicitsName, verifiedKraptrStringName,
      DSLCompiler.behaviourDefinitionClassName)
    val methodFilters = allowedClasses.map(x => s"CallTargets.AllMethodsOf[$x]")
    val restrictDeclaration = methodFilters.mkString(s"Restrict.targets[$verifiedDeciderName,", " + ",
      "]")
    val opening = (0 to 20).map(_ => "{").mkString("")
    val closing = (0 to 20).map(_ => "}").mkString("")
    Success(s"$restrictDeclaration $opening\n\t${dslAndTags.dsl}\n$closing")

  }
}
