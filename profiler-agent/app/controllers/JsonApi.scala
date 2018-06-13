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

import domain.Errors
import job.runner.ProfilerAgentException
import org.apache.commons.lang3.exception.ExceptionUtils
import play.api.Logger
import play.api.libs.json.{Json, Writes}
import play.api.mvc.{Controller, Result}

import scala.concurrent.Future

trait JsonApi {
  self: Controller =>

  import domain.JsonFormatters._

  def mapErrorWithLog(logBlock : (Throwable) => Unit = defaultLogMessage): PartialFunction[Throwable, Result] = {
    case e: Throwable =>{
      logBlock(e)
      mapError.apply(e)
    }
  }

  private def defaultLogMessage =
    (e : Throwable) => Logger.error(e.getMessage, e)

  def mapError: PartialFunction[Throwable, Result] = {
    case e: ProfilerAgentException =>
      InternalServerError(Json.toJson(Errors(e.code, e.message)))
    case (th: Throwable) =>
      InternalServerError(Json.toJson(Errors(th.getMessage, ExceptionUtils.getStackTrace(th))))
  }
}
