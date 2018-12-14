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


package com.hortonworks.dataplane.profilers.instances

import java.nio.charset.Charset

import com.hortonworks.dataplane.profilers.commons.Models._
import com.hortonworks.dataplane.profilers.commons.Parsers._
import com.hortonworks.dataplane.profilers.commons.{AppConfig, CommonUtils, StackableException}
import com.hortonworks.dataplane.profilers.kraptr.dsl.TagCreator
import com.hortonworks.dataplane.profilers.kraptr.engine.loader.BehaviourLoader
import com.hortonworks.dataplane.profilers.kraptr.engine.repl.DSLCompiler
import com.hortonworks.dataplane.profilers.kraptr.models.dsl.{DSLandTags, MatchType}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import play.api.libs.json.Json

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

class DryRunSensitivityProfiler(config: AppConfig,
                                settings: DryRunSettings,
                                path: String) extends LazyLogging with SensitivityProfiler {
  def triggerProfiler(): Unit = {
    BehaviourLoader.loadBehaviours(config.dslContext).map(
      DSLCompiler.compileAndCreateTagCreators(_, Nil, config.dslContext)
    ) match {
      case Success(_) =>
        val outputSchema: Try[DryRunOutPutSchema] = collectDryRunResults(settings.dryRunRules).map(
          results => {
            DryRunOutPutSchema(DryRunOutPutStatus.SUCCESSFUL, None, Some(results))
          }
        ) recover {
          case stackableException: StackableException =>
            val cause = Cause("Failed process DSL rules" +: stackableException.errorMessages, CommonUtils.convertStackTraceToString(stackableException.exception))
            DryRunOutPutSchema(DryRunOutPutStatus.FAILURE,
              Some(cause), None)
          case exception: Exception =>
            val cause = Cause(List("Failed process DSL rules"), CommonUtils.convertStackTraceToString(exception))
            DryRunOutPutSchema(DryRunOutPutStatus.FAILURE,
              Some(cause), None)
        }
        outputSchema.map(
          schema => {
            writeOutputSchemaToHDFSFile(schema)
          }
        ).get
      case Failure(exception) =>
        val errorMessage = "Failed to load custom Behaviours"
        val cause = Cause(List(errorMessage), CommonUtils.convertStackTraceToString(exception))
        writeOutputSchemaToHDFSFile(DryRunOutPutSchema(DryRunOutPutStatus.FAILURE, Some(cause), None))
    }
  }


  private def writeOutputSchemaToHDFSFile(schema: DryRunOutPutSchema) = {
    val outputFileContent = Json.stringify(Json.toJson(schema))
    val hdfsOutputFile = new Path(path)
    config.hdfsFileSystem.delete(hdfsOutputFile, false)
    val stream: FSDataOutputStream = config.hdfsFileSystem.create(hdfsOutputFile)
    stream.write(outputFileContent.getBytes(Charset.forName("UTF-8")))
    stream.close()
  }

  @tailrec
  private def collectDryRunResults(dryRunRules: List[DryRunRule], results: List[DryRunResult] = Nil): Try[List[DryRunResult]] = {
    if (dryRunRules.isEmpty) {
      Success.apply(results)
    }
    else {
      loadDSLs(dryRunRules.head.dsls) match {
        case Success(tagCreators) =>
          val triedResult: Try[DryRunResult] = Try {
            val data = dryRunRules.head.data
            val columnsAndTags: Set[(String, String)] = data.flatMap(datapoint => {
              tagCreators.flatMap(_ apply datapoint)
                .map(datapoint -> _)
            }).toSet
            val dataAndTags = columnsAndTags.groupBy(_._1).mapValues(_.map(_._2))
              .map(x => DataAndTags(x._1, x._2.toList)).toList
            DryRunResult(dryRunRules.head.name, dataAndTags)
          } recoverWith {
            case stackableException: StackableException =>
              Failure.apply(StackableException(s"Failed to apply dry-run rule ${dryRunRules.head.name} on data" +: stackableException.errorMessages, stackableException.exception))
            case x =>
              Failure.apply(StackableException(List(s"Failed to apply dry-run rule ${dryRunRules.head.name} on data "), x))
          }

          triedResult match {
            case Success(dryRunResult) =>
              collectDryRunResults(dryRunRules.tail, dryRunResult +: results)
            case Failure(x) =>
              Failure(x)
          }
        case Failure(exception) =>
          exception match {
            case stackableException: StackableException =>
              Failure.apply(StackableException(s"Failed to load dry-run rules ${dryRunRules.head.name}" +: stackableException.errorMessages, stackableException.exception))
            case x =>
              Failure.apply(StackableException(List(s"Failed to load dry-un rules ${dryRunRules.head.name}"), x))
          }
      }
    }
  }


  @tailrec
  private def loadDSLs(dsls: List[DryRunDSLAndTag], tagCreators: List[TagCreator] = Nil): Try[List[TagCreator]] = {

    if (dsls.isEmpty) {
      Success.apply(tagCreators)
    }
    else {
      DSLCompiler.compileAndCreateTagCreators(Nil, List(DSLandTags(MatchType.value, dsls.head.dsl, dsls.head.tags, 100.0)), config.dslContext) match {
        case Success(x) => loadDSLs(dsls.tail, x.head +: tagCreators)
        case Failure(exception) =>
          Failure.apply(StackableException(List(s"Failed to load DSL \n ${dsls.head.dsl}"), exception))
      }
    }
  }


}
