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

import domain.ProfilerInstance
import job.submitter.{ProfilerSubmitterManager, SubmitterRegistry}
import org.joda.time.DateTime
import play.api.Logger
import play.api.libs.json.{JsError, JsSuccess, Json}
import play.api.mvc.{Action, Controller}
import repo.ProfilerInstanceRepo
import views.html._

import scala.concurrent.{ExecutionContext, Future}

@Singleton
class ProfilerInstances @Inject()(profilerInstanceRepo: ProfilerInstanceRepo, submitterManager: ProfilerSubmitterManager)(implicit exec: ExecutionContext)
  extends Controller with JsonApi {

  import domain.JsonFormatters._

  def indexAction = Action {
    Ok(index.render())
  }

  def all = Action.async {
    profilerInstanceRepo.findAll().map(profilers => Ok(Json.toJson(profilers)))
      .recover(mapError)
  }

  def getByName(name: String) = Action.async {
    profilerInstanceRepo.findByNameOpt(name).map {
      profilerOpt =>
        profilerOpt.map(profiler => Ok(Json.toJson(profiler))).getOrElse(NotFound)
    }.recover(mapError)
  }

  def insert = Action.async(parse.json) { req =>
    req.body
      .validate[ProfilerInstance] match {
      case JsSuccess(profiler, path) =>
        profilerInstanceRepo.save(profiler.copy(version = 1, created = Some(DateTime.now()))).map {
          p =>
            if (p.profilerInstance.active) {
              submitterManager.addProfilerInstance(p)
            }
            Ok(Json.toJson(p))
        }.recover(mapError)
      case e: JsError =>
        Future.successful(BadRequest(JsError.toFlatForm(e).toString()))
    }
  }

  def updateState(name: String, active: Boolean) = Action.async { req =>
    profilerInstanceRepo.findByNameOpt(name).flatMap {
      case Some(profiler) =>
        if (profiler.profilerInstance.active != active) {
          profilerInstanceRepo.updateState(name, active)
            .map { e =>
              if (active) {
                Logger.info(s"Marking profiler : ${name} as active")
                submitterManager.addProfilerInstance(e)
              } else {
                Logger.info(s"Marking profiler : ${name} as inactive")
                submitterManager.stopProfilerInstance(e.profilerInstance.name)
              }
              Ok(Json.obj(
                "name" -> name,
                "state" -> active,
                "changed" -> true
              ))
            }
        } else Future.successful(Ok(Json.obj(
          "name" -> name,
          "state" -> active,
          "changed" -> false
        )))
      case None => Future.successful(NotFound)
    }
  }


  def update = Action.async(parse.json) { req =>
    req.body
      .validate[ProfilerInstance] match {
      case JsSuccess(profiler, path) =>
        profilerInstanceRepo.update(profiler).map {
          p =>
            if (p.profilerInstance.active) {
              submitterManager.updateProfilerInstance(p)
            }
            Ok(Json.toJson(p))
        }.recover(mapError)
      case e: JsError =>
        Future.successful(BadRequest(JsError.toFlatForm(e).toString()))
    }
  }
}
