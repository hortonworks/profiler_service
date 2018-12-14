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

import domain.{CacheMetrics, Errors, InteractiveQuery}
import javax.inject.{Inject, Singleton}

import job.interactive.{LivyInteractiveRunner, LivyQueryCache, LivySessionNotRunningException}
import job.runner.ProfilerAgentException
import org.apache.commons.lang3.exception.ExceptionUtils
import play.api.libs.json.{JsError, JsSuccess, Json}
import play.api.mvc.{Action, Controller}

import scala.concurrent.{ExecutionContext, Future}


@Singleton
class InteractiveMetrics @Inject()(livyInteractiveRunner: LivyInteractiveRunner,
                                   livyQueryCache: LivyQueryCache
                                  )(implicit exec: ExecutionContext) extends Controller with JsonApi {

  import domain.JsonFormatters._

  def submitInteractiveQuery() = Action.async(parse.json) { req =>
    req.body
      .validate[InteractiveQuery] match {
      case JsSuccess(query, path) =>
        livyInteractiveRunner.postInteractiveQueryAndGetResult(query).map(d => Accepted(Json.obj("data" -> d)))
          .recover {
            case ex: LivySessionNotRunningException =>
              ServiceUnavailable(Json.toJson(Errors(ex.getMessage, ExceptionUtils.getStackTrace(ex))))
            case (th: Throwable) =>
              InternalServerError(Json.toJson(Errors(th.getMessage, ExceptionUtils.getStackTrace(th))))
          }
      case e: JsError =>
        Future.successful(BadRequest(JsError.toFlatForm(e).toString()))
    }
  }

  // get or create from cache
  def getOrSubmitInteractiveQuery() = Action.async(parse.json) { req =>
    req.body
      .validate[InteractiveQuery] match {
      case JsSuccess(query, path) =>

        livyQueryCache.get(query)
          .map(d => Accepted(Json.obj("data" -> d)))
          .recover {
            case ex: LivySessionNotRunningException =>
              ServiceUnavailable(Json.toJson(Errors(ex.getMessage, ExceptionUtils.getStackTrace(ex))))
            case (th: Throwable) =>
              InternalServerError(Json.toJson(Errors(th.getMessage, ExceptionUtils.getStackTrace(th))))
          }
      case e: JsError =>
        Future.successful(BadRequest(JsError.toFlatForm(e).toString()))
    }
  }


  def cacheMetrics() = Action.async(parse.json) { req =>
    req.body
      .validate[CacheMetrics] match {
      case JsSuccess(query, _) =>
        livyInteractiveRunner.cacheMetrics(query).map(d => Accepted(Json.obj("data" -> d)))
          .recover {
            case ex: LivySessionNotRunningException =>
              ServiceUnavailable(Json.toJson(Errors(ex.getMessage, ExceptionUtils.getStackTrace(ex))))
            case (th: Throwable) =>
              InternalServerError(Json.toJson(Errors(th.getMessage, ExceptionUtils.getStackTrace(th))))
          }
      case e: JsError =>
        Future.successful(BadRequest(JsError.toFlatForm(e).toString()))
    }
  }

  def assets(action: String) = Action.async { req =>
    action match {
      case "refresh" =>
        livyQueryCache.clearCache().flatMap(
          _ =>
            livyInteractiveRunner.refreshAllTables().map(Accepted(_))
              .recover {
                case ex: LivySessionNotRunningException =>
                  ServiceUnavailable(Json.toJson(Errors(ex.getMessage, ExceptionUtils.getStackTrace(ex))))
                case (th: Throwable) =>
                  InternalServerError(Json.toJson(Errors(th.getMessage, ExceptionUtils.getStackTrace(th))))
              }
        )
      case _ =>
        Future.successful(BadRequest("No action found for asset"))
    }
  }

  def status(sessionType: String = "read") = Action.async { req =>
    livyInteractiveRunner.sessionStatus(sessionType)
      .map(e => Ok(Json.obj("sessionid" -> e)))
      .recover {
        case ex: ProfilerAgentException =>
          new Status(ex.code.toInt)(Json.toJson(Errors(ex.getMessage, ExceptionUtils.getStackTrace(ex))))
        case ex: Throwable =>
          InternalServerError(Json.toJson(Errors(ex.getMessage, ExceptionUtils.getStackTrace(ex))))
      }
  }
}
