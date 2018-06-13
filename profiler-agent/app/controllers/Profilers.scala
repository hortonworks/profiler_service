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

import domain.Profiler
import org.joda.time.DateTime
import play.api.libs.json.{JsError, JsSuccess, Json}
import play.api.mvc.{Action, Controller}
import repo.ProfilerRepo
import views.html._

import scala.concurrent.{ExecutionContext, Future}

@Singleton
class Profilers @Inject()(profilerRepo: ProfilerRepo)(implicit exec: ExecutionContext)
  extends Controller with JsonApi {

  import domain.JsonFormatters._

  def indexAction = Action {
    Ok(index.render())
  }

  def all = Action.async {
    profilerRepo.findAll().map(profilers => Ok(Json.toJson(profilers)))
      .recover(mapError)
  }

  def getById(id: Long) = Action.async {
    profilerRepo.findOne(id).map {
      profilerOpt =>
        profilerOpt.map(profiler => Ok(Json.toJson(profiler))).getOrElse(NotFound)
    }.recover(mapError)
  }

  def insert = Action.async(parse.json) { req =>
    req.body
      .validate[Profiler] match {
      case JsSuccess(profiler, path) =>
        profilerRepo.save(profiler.copy(created = Some(DateTime.now()))).map(p => Ok(Json.toJson(p)))
          .recover(mapError)
      case e: JsError =>
        Future.successful(BadRequest(JsError.toFlatForm(e).toString()))

    }
  }

  def update = Action.async(parse.json) { req =>
    req.body
      .validate[Profiler] match {
      case JsSuccess(profiler, path) =>
        val pc = profiler.copy(created = Some(DateTime.now()))
        profilerRepo.update(pc).map(p => Ok(Json.toJson(pc)))
          .recover(mapError)
      case e: JsError =>
        Future.successful(BadRequest(JsError.toFlatForm(e).toString()))

    }
  }
}
