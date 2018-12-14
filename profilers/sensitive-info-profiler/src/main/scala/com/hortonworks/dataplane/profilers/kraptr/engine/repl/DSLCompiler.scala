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
import com.hortonworks.dataplane.profilers.kraptr.dsl.TagCreator
import com.hortonworks.dataplane.profilers.kraptr.dsl.transformers.{BehaviourTransformer, DSLTransformer}
import com.hortonworks.dataplane.profilers.kraptr.models.behaviour.BehaviourDefinition
import com.hortonworks.dataplane.profilers.kraptr.models.dsl.DSLandTags
import org.apache.spark.sql.SparkSession

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

object DSLCompiler {

  val behaviourDefinitionClassName = "KraptrBehaviourDefinitions"

  val behaviourDefinitionInstanceName = "kraptrBehaviourDefinitions"


  def compileAndCreateTagCreators(behaviours: Seq[BehaviourDefinition],
                                  dslAndTags: List[DSLandTags],
                                  context: KraptrContext): Try[List[TagCreator]] = {

    val interpreter = new Interpreter(context)

    bindSparkSessionAndImportImplicitly(context.spark, interpreter).flatMap(_ =>
      loadSecurityRelatedImports(interpreter).flatMap(_ => loadBasic(interpreter)).flatMap(_ =>
        loadBehaviours(interpreter, behaviours)
      ).flatMap(_ => loadTagCreators(interpreter, dslAndTags))
    ).map(
      x => {
        interpreter.close()
        x
      }
    )

  }


  private def bindSparkSessionAndImportImplicitly(
                                                   sparkSession: SparkSession,
                                                   interpreter: Interpreter): Try[Unit] = {
    val name = "kraptrSparkSession"
    val tempBindingName = name + "_temp"
    interpreter.createBinding[SparkSession](sparkSession, tempBindingName).flatMap(
      _ => interpreter.define(s"import org.apache.spark.sql.SparkSession\n" +
        s"implicit val $name=$tempBindingName.asInstanceOf[SparkSession]")
    )
  }


  private def loadBasic(interpreter: Interpreter): Try[Unit] = {
    loadImplicits(interpreter).flatMap(_ => loadSpecialBehaviours(interpreter))
      .flatMap(_ => loadInbuiltBehaviours(interpreter))
  }


  @tailrec
  private def loadTagCreators(interpreter: Interpreter, dslAndTags: List[DSLandTags], accumulator: List[TagCreator] = List.empty[TagCreator]): Try[List[TagCreator]] = {
    if (dslAndTags.isEmpty) {
      Success(accumulator)
    }
    else {
      val dslAndTag = dslAndTags.head
      val tryCompilableCode = DSLTransformer.toCompilableCode(dslAndTag)
      tryCompilableCode match {
        case Success(compilableCode) =>
          interpreter.buildBehaviour(compilableCode).map(
            _.thenTagAs(dslAndTag.matchType, dslAndTag.tags,dslAndTag.confidence)
          ) match {
            case Failure(error) =>
              Failure(error)
            case Success(tagCreator) =>
              loadTagCreators(interpreter, dslAndTags.tail, accumulator :+ tagCreator)
          }
        case Failure(error) =>
          Failure(error)
      }
    }
  }

  @tailrec
  private def loadBehaviours(interpreter: Interpreter, behaviours: Seq[BehaviourDefinition], methodDefinitions: List[String]
  = List.empty[String]): Try[Unit] = {
    if (behaviours.isEmpty) {

      val definition = methodDefinitions.mkString(s"class $behaviourDefinitionClassName {\n", "\n\t", "\n}\n")

      val importMethods = s"val $behaviourDefinitionInstanceName= new $behaviourDefinitionClassName()" +
        s"\n import $behaviourDefinitionInstanceName._"

      val defineCode = definition + importMethods
      interpreter.define(defineCode)
    }
    else {
      val behaviour = behaviours.head
      val tryCompilableCode = BehaviourTransformer.toCompilableDefinitionAndMethodToLoad(behaviour)
      tryCompilableCode match {
        case Success(compilableCodeAndMethod) =>
          interpreter.define(compilableCodeAndMethod.definition,
            s"Failed to compile behaviour $behaviour") match {
            case Failure(error) =>
              Failure(error)
            case Success(_) =>
              val methodDefinitionsTemp = compilableCodeAndMethod.optionalMethod match {
                case Some(methodDef) => methodDefinitions :+ methodDef
                case None => methodDefinitions
              }
              loadBehaviours(interpreter, behaviours.tail, methodDefinitionsTemp)
          }
        case Failure(error) =>
          Failure(error)
      }
    }
  }

  private def loadImplicits(interpreter: Interpreter): Try[Unit] = {
    val code = "val kraptrImplicits =new com.hortonworks.dataplane.profilers.kraptr.dsl.Implicits()\n" +
      "import kraptrImplicits._"
    interpreter.define(code, "Failed to load implicits")
  }

  private def loadSpecialBehaviours(interpreter: Interpreter): Try[Unit] = {
    val code = "val  specialOperations=new com.hortonworks.dataplane.profilers.kraptr.behaviour.SpecialOperations()\n" +
      "import specialOperations._"
    interpreter.define(code, "Failed to load special operations")
  }

  private def loadInbuiltBehaviours(interpreter: Interpreter): Try[Unit] = {
    val code = "val  inbuiltBehaviours=new com.hortonworks.dataplane.profilers.kraptr.behaviour.inbuilt.InbuiltBehaviours()\n" +
      "import inbuiltBehaviours._\n" +
      "import com.hortonworks.dataplane.profilers.kraptr.behaviour.inbuilt.instances._"
    interpreter.define(code, "Failed to load inbuilt behaviours")
  }

  private def loadSecurityRelatedImports(interpreter: Interpreter): Try[Unit] = {
    val code =
      s"""import com.fsist.subscala.CallTargets.+
                  import com.fsist.subscala.{CallTargets, Restrict}"""
    interpreter.define(code, "Failed to load s ecurity related imports")
  }


}
