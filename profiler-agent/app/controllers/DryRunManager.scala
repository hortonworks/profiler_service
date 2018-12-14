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

import commons.CommonUtils._
import javax.inject.{Inject, Singleton}
import play.api.libs.json.{JsError, JsSuccess, Json}
import play.api.mvc.{Action, Controller, Result}
import profilers.dryrun.DryRunManagementService
import profilers.dryrun.errorhandling.{DryRunNotFoundException, FailedToSubmitDryRunToLivyException, ProfilerInstanceMissingException, ProfilerMissingException}
import profilers.dryrun.models.Requests.{ClearDryRun, GetDryRun, TriggerDryRun}

import scala.concurrent.{ExecutionContext, Future}

@Singleton
class DryRunManager @Inject()(dryRunManagementService: DryRunManagementService)
                             (implicit exec: ExecutionContext)
  extends Controller with JsonApi {

  import profilers.dryrun.parsers.RequestParsers._
  import profilers.dryrun.parsers.ResponseParsers._


  private def errorToResult(throwable: Throwable): Result = throwable match {
    case error: ProfilerMissingException => BadRequest(convertStackTraceToString(error))
    case error: ProfilerInstanceMissingException => BadRequest(convertStackTraceToString(error))
    case error: FailedToSubmitDryRunToLivyException => ServiceUnavailable(convertStackTraceToString(error))
    case error: DryRunNotFoundException => NotFound(convertStackTraceToString(error))
    case error => InternalServerError(convertStackTraceToString(error))
  }

  def triggerDryRun() = Action.async(parse.json) {
    req =>
      req.body
        .validate[TriggerDryRun] match {
        case JsSuccess(query, _) =>
          dryRunManagementService.triggerDryRun(query)
            .map(x => Ok(Json.toJson(x)))
            .recover {
              case x: Throwable => errorToResult(x)
            }
        case e: JsError =>
          Future.successful(BadRequest(JsError.toFlatForm(e).toString()))
      }
  }


  def getDryRun(id: Long) = Action.async {
    dryRunManagementService.getStatus(GetDryRun(id))
      .map(x => Ok(Json.toJson(x)))
      .recover {
        case x: Throwable => errorToResult(x)
      }
  }


  def clearDryRun(id: Long) = Action.async {
    dryRunManagementService.clearDryRun(ClearDryRun(id))
      .map(_ => Ok(s"Dry run $id removed"))
      .recover {
        case x: Throwable => errorToResult(x)
      }
  }


}
