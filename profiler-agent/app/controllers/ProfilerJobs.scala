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

import javax.inject.Inject

import domain._
import job.manager.JobManager
import org.joda.time.DateTime
import play.api.Logger
import play.api.libs.json.{JsError, JsSuccess, Json}
import play.api.mvc.{Action, Controller}
import repo.ProfilerJobRepo

import scala.annotation.tailrec
import scala.concurrent.Future

class ProfilerJobs @Inject()(jobManager: JobManager,
                             profilerJobRepo: ProfilerJobRepo) extends Controller with JsonApi {

  import domain.JsonFormatters._

  import scala.concurrent.ExecutionContext.Implicits.global

  def startJob() = Action.async(parse.json) { req =>
    req.body
      .validate[JobTask] match {
      case JsSuccess(jobTask, path) =>
        jobManager.schedule(jobTask)
          .map(e => Ok(Json.toJson(e)))
          .recover(mapError)
      case e: JsError =>
        Future.successful(BadRequest(JsError.toFlatForm(e).toString()))
    }
  }

  def status(id: Long) = Action.async {
    jobManager.status(id)
      .map(e => Ok(Json.toJson(e)))
      .recover(mapError)
  }

  def getAllJobs(skip: Long, limit: Long, submitter: Option[String], scheduled: Option[Boolean]) = Action.async {
    profilerJobRepo.getJobs(skip, limit, submitter, scheduled)
      .map(j => Ok(Json.toJson(j)))
      .recover(mapError)
  }

  def kill(id: Long) = Action.async {
    jobManager.kill(id)
      .map(e => Ok(Json.toJson(e)))
      .recover(mapError)
  }

//  def getLastStatus(assetId: String, profilerName: String) = Action.async {
//    profilerRepo.getByName(profilerName).flatMap {
//      case Some(p) =>
//        assetRunHistoryRepo.getLastJob(assetId, p.id.get).map {
//          case Some(history) => Ok(Json.toJson(history))
//          case None => NotFound
//        }
//      case None =>
//        Future.successful(BadRequest(s"Profiler ${profilerName} not found"))
//    }.recover(mapError)
//  }
//
//  def getLastStatusForSeq(assetIds: String, profilerName: String) = Action.async {
//    profilerRepo.getByName(profilerName).flatMap {
//      case Some(p) =>
//        assetRunHistoryRepo.getAllInSeq(assetIds.split(",").toSeq, p.id.get).map {
//          list => Ok(Json.toJson(list))
//        }
//      case None =>
//        Future.successful(BadRequest(s"Profiler ${profilerName} not found"))
//    }.recover {
//      mapError
//    }
//  }

  def getAssetsCount(startTimeMs: Long, endTimeMs: Long) = Action.async {

    Logger.info("ProfilerJobs Controller: Received getAssetsCount request")

    val startDateTime = new DateTime(startTimeMs)
    val endDateTime =  new DateTime(endTimeMs)

    profilerJobRepo.getOperatedAssetsCount(startDateTime, endDateTime).map { results =>
      Ok(Json.toJson(results))
    }.recover(mapErrorWithLog(e => Logger.error(s"ProfilerJobs Controller: getAssetsCount with params start day $startTimeMs and end day $endTimeMs failed with message ${e.getMessage}", e)))

  }

  def getJobCounts(startTimeMs: Long, endTimeMs: Long) = Action.async {

    Logger.info("ProfilerJobs Controller: Received getJobCounts request")

    val startDateTime = new DateTime(startTimeMs)
    val endDateTime = new DateTime(endTimeMs)

    profilerJobRepo.getJobCounts(startDateTime, endDateTime).map { results =>

      Ok(Json.toJson(results))

    }.recover(mapErrorWithLog(e => Logger.error(s"ProfilerJobs Controller: getJobCounts with params start day $startTimeMs and end day $endTimeMs failed with message ${e.getMessage}", e)))
  }

  def getJobsOnFilters(startTimeMs: Option[Long], endTimeMs: Option[Long], status: Seq[String], profilerIds: Seq[Long], offset: Option[Int], limit: Option[Int], sortBy: Option[String], sortDir: Option[String]) = Action.async {

    Logger.info("ProfilerJobs Controller: Received getJobsOnFilters request")

    profilerJobRepo.getJobsOnFilters(startTimeMs, endTimeMs, status, profilerIds, getPaginatedQuery(offset, limit, sortBy, sortDir)).map {results =>

      Ok(Json.toJson(results))
    }
      .recover(mapErrorWithLog(e => Logger.error(s"ProfilerJobs Controller: getJobsOnFilters with params $status, $profilerIds, $offset, $limit, $sortDir and $sortBy failed with message ${e.getMessage}", e)))

  }

  private def getPaginatedQuery(offset: Option[Int], limit: Option[Int], sortBy: Option[String], sortDir: Option[String]) ={

    val sortQuery = sortBy.map(s => SortQuery(s, sortDir.getOrElse("asc")))
    Some(PaginatedQuery(offset.getOrElse(0), limit.getOrElse(50), sortQuery))

  }

}
