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

import com.hortonworks.dataplane.profilers.kraptr.behaviour.Decider
import com.hortonworks.dataplane.profilers.kraptr.models.behaviour.{BehaviourConfigurationType, BehaviourDefinition}
import com.typesafe.scalalogging.LazyLogging

import scala.util.{Failure, Success, Try}

object BehaviourTransformer extends LazyLogging {

  private val kraptrTransformedSuffix = "_kraptr_behaviour_impl"

  private val nameRegex: String = "([a-z][a-z0-9]+)+"


  def toCompilableDefinitionAndMethodToLoad(behaviour: BehaviourDefinition): Try[DefinitionAndOptionalMethod] = {

    val verifiedDeciderName = classOf[Decider].getCanonicalName
    val imports = getImports()

    val tryBasicDeclaration: Try[(String, Option[String])] = behaviour.configurationType match {
      case BehaviourConfigurationType.none =>
        Success((s"object ${behaviour.name} extends $verifiedDeciderName{", None))
      case BehaviourConfigurationType.map =>
        val extractedName = s"${behaviour.name + kraptrTransformedSuffix}"
        val methodDefinition = s"def ${behaviour.name}(configs: Map[String, String])=$extractedName(configs)"
        Success((s"case class $extractedName(configs: Map[String, String])(implicit spark: SparkSession) extends $verifiedDeciderName(){", Some(methodDefinition)))
      case BehaviourConfigurationType.list =>
        val extractedName = s"${behaviour.name + kraptrTransformedSuffix}"
        val methodDefinition = s"def ${behaviour.name}(configs:String*)=$extractedName(configs)"
        Success((s"case class $extractedName(configs:Seq[String])(implicit spark: SparkSession) extends $verifiedDeciderName(){",
          Some(methodDefinition)))
      case x =>
        val errorMessage = s"unrecogonized configuratioon type $x in $behaviour"
        logger.error(errorMessage)
        Failure(new Exception(errorMessage))
    }


    tryBasicDeclaration.map(basicDeclarationAndMethodDef => {
      val functionDeclaration = "override def process(token: String): Boolean = {"


      DefinitionAndOptionalMethod(s"$imports\n\n\n${basicDeclarationAndMethodDef._1}\n\n\t$functionDeclaration\n\t\t${behaviour.definition}\n\t}\n}"
        , basicDeclarationAndMethodDef._2)
    }
    )
  }


  private def getImports(): String = {
    val verifiedSparkSessionCanonicalName = "org.apache.spark.sql.SparkSession"
    s"import $verifiedSparkSessionCanonicalName"
  }

  def checkForValidityOfName(behaviour: BehaviourDefinition): Boolean = {
    behaviour.name.matches(nameRegex)
  }
}
