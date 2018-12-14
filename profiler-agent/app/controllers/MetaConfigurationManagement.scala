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

package controllers

import commons.CommonUtils
import commons.CommonUtils._
import javax.inject.{Inject, Singleton}
import play.api.libs.Files
import play.api.libs.json.{JsError, JsSuccess, Json}
import play.api.mvc.{Action, Controller, Result}
import profilers.metadata.MetaDataManagementService
import profilers.metadata.errorhandling.{InvalidRequestException, NodeAlreadyExistsException, NodeNotFoundException, NotAllowedException}
import profilers.metadata.models.ProfilerMetaConfiguration
import profilers.metadata.models.Requests._
import repo.ProfilerRepo

import scala.concurrent.{ExecutionContext, Future}

@Singleton
class MetaConfigurationManagement @Inject()(profilerRepo: ProfilerRepo)
                                           (implicit exec: ExecutionContext)
  extends Controller with JsonApi {


  val service: MetaDataManagementService = new MetaDataManagementService(
    CommonUtils.fileSystem
  )

  import profilers.metadata.parsers.RequestParsers._
  import profilers.metadata.parsers.ResponseParsers._

  private def metaConfigSchema: Future[Map[String, ProfilerMetaConfiguration]] = {
    profilerRepo.findAll().map(profilers => {
      profilers.flatMap(
        profiler => {
          profiler.metaManagementSchema.map(
            profiler.name -> _
          )
        }
      ).toMap
    })
  }


  private def errorToResult(throwable: Throwable): Result = throwable match {
    case error: NodeAlreadyExistsException => Conflict(convertStackTraceToString(error))
    case error: NodeNotFoundException => NotFound(convertStackTraceToString(error))
    case error: InvalidRequestException => BadRequest(convertStackTraceToString(error))
    case error: NotAllowedException => Forbidden(convertStackTraceToString(error))
    case error => InternalServerError(convertStackTraceToString(error))
  }

  def listNodes(instance: Option[String],
                profilerId: String, id: String
                , startsWith: Option[String]) = Action.async {
    val context = MetaContext(instance, profilerId, id)
    val listNodes = ListNodes(context, startsWith)
    metaConfigSchema.flatMap(
      service.listNodes(_)(listNodes)
        .map(e => Ok(Json.toJson(e)))
    ) recover {
      case x: Throwable => errorToResult(x)
    }
  }


  def readNodeContent(instance: Option[String],
                      profilerId: String
                      , id: String, nodeName: String) = Action.async {
    val context = MetaContext(instance, profilerId, id)
    val readContent = ReadNodeContent(context, nodeName)
    metaConfigSchema.flatMap(
      service.readNodeContent(_)(readContent)
        .map(e => Ok(e))
    ) recover {
      case x => errorToResult(x)
    }
  }

  def overWriteNodeContent(instance: Option[String],
                           profilerId: String, id: String
                           , nodeName: String) = Action.async(parse.text) { content =>
    val context = MetaContext(instance, profilerId, id)
    metaConfigSchema.flatMap(
      service.overwriteNodeContent(_)(OverwriteNodeContent(context, nodeName, content.body))
        .map(e => Ok(Json.toJson(e)))
    ) recover {
      case x => errorToResult(x)
    }
  }


  def createNode(instance: Option[String],
                 profilerId: String, id: String
                 , nodeName: String) = Action.async(parse.text) { content =>
    val context = MetaContext(instance, profilerId, id)
    metaConfigSchema.flatMap(
      service.createNode(_)(CreateNode(context, nodeName, content.body))
        .map(e => Ok(Json.toJson(e)))
    ) recover {
      case x => errorToResult(x)
    }
  }

  def uploadNode(instance: Option[String],
                 profilerId: String, id: String
                 , nodeName: String) = Action.async(parse.multipartFormData) { request =>

    request.body.file("metafile").map { metafile =>
      val temporaryFile: Files.TemporaryFile = metafile.ref
      val context = MetaContext(instance, profilerId, id)
      metaConfigSchema.flatMap(
        service.uploadNode(_)(UploadNode(context, nodeName, temporaryFile))
          .map(e => Ok(Json.toJson(e)))
      )
    }.getOrElse {
      Future.successful(NotFound("missing file metafile"))
    } recover {
      case x => errorToResult(x)
    }
  }


  def deleteNode(instance: Option[String],
                 profilerId: String
                 , id: String,
                 nodeName: String) = Action.async {
    val context = MetaContext(instance, profilerId, id)
    val deleteNode = DeleteNode(context, nodeName)
    metaConfigSchema.flatMap(
      service.deleteNode(_)(deleteNode)
        .map(_ => Ok(s"Node ${deleteNode.nodeName} Successfully deleted"))
    ) recover {
      case x => errorToResult(x)
    }
  }


  def getNodeInfo() = Action.async(parse.json) { req =>
    req.body
      .validate[GetInformationAboutNodes] match {
      case JsSuccess(query, _) =>
        metaConfigSchema.flatMap(
          service.getNodeInfo(_)(query).map(e => Ok(Json.toJson(e)))
        ) recover {
          case x => errorToResult(x)
        }
      case e: JsError =>
        Future.successful(BadRequest(JsError.toFlatForm(e).toString()))
    }
  }


}
