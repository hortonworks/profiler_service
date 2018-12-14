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

import javax.inject.{Inject, Singleton}

import job.incremental.ChangeDetectorJob
import job.incremental.source.HiveClientChangeEventSource
import play.api.{Configuration, Logger}
import play.api.libs.json.{JsError, JsSuccess, Json}
import play.api.mvc.{Action, Controller}
import repo.ChangeNotificationLogRepo

import scala.concurrent.{ExecutionContext, Future}

@Singleton
class IncrementalChangeDetectors @Inject()(changeNotificationLogRepo: ChangeNotificationLogRepo, configuration: Configuration)(implicit exec: ExecutionContext)
  extends Controller with JsonApi{

  import domain.JsonFormatters._

  val logger = Logger(classOf[IncrementalChangeDetectors])
  def getAllChanges(startTime: Option[Long], endTime: Option[Long]) = Action.async {

    logger.info("Retrieving all logged change events")
    changeNotificationLogRepo.getAllInRange(startTime, endTime).map { res =>
      Ok(Json.toJson(res))
    }
      .recover(mapErrorWithLog(e => logger.error(s"retrieving logged events failed", e)))
  }

  def startChangeDetector() = Action.async(parse.json) { req =>

    logger.info("Starting change detector of incremental profiling")
    val json = req.body
    (json \ "purgeOldLogs").validateOpt[Boolean] match {
      case JsSuccess(purge, path) => {
        val purgeOld = purge.getOrElse(true)
        val changeEventSource = HiveClientChangeEventSource(configuration, changeNotificationLogRepo)
        val keepRangeMs = (configuration.getLong("dpprofiler.incremental.changedetector.keeprange").getOrElse(86400L)) * 1000
        ChangeDetectorJob.logChangeEvents(changeEventSource, changeNotificationLogRepo, keepRangeMs, purgeOld).map { res =>
          Ok(Json.obj("message" -> "success"))
        }
      }
      case e: JsError =>
        logger.error("Failed to map request body as boolean")
        Future.successful(BadRequest(JsError.toFlatForm(e).toString()))
    }
  }
}
