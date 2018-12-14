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

package job.manager

import akka.actor.Actor
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import domain.JobStatus.JobStatus
import domain._
import job.interactive.CacheRefreshHandle
import play.api.Logger
import repo.AssetJobHistoryRepo

import scala.util.{Failure, Success, Try}


class JobManagerActor(implicit val materializer: ActorMaterializer, jobService: JobService,
                      assetJobHistoryRepo: AssetJobHistoryRepo,
                      jobRefreshTime: Int, cacheRefreshHandler: CacheRefreshHandle) extends Actor {

  private val managedJobs = collection.mutable.Map[Long, ProfilerJob]()

  import akka.pattern.pipe

  import concurrent.duration._
  import scala.concurrent.ExecutionContext.Implicits.global

  context.system.scheduler.schedule(5 seconds, jobRefreshTime seconds, self, CheckStatusForAll)

  private var statusCheckInProgress = false

  init()

  private def init() = {
    assetJobHistoryRepo.getAllRunningOrStarted().map {
      history =>
        history.foreach {
          h =>
            jobService.jobStatus(h.jobId) map {
              resp =>
                self ! updateJob(resp)
            }
        }
    }
  }

  private def checkStatusForAll() = {
    Logger.info(s"Checking status for all jobs. Number of Jobs : ${managedJobs.size}")
    Source(managedJobs.values.toList)
      .mapAsync(3)(e => jobService.jobStatus(e.id.get)
        .map(t => (e, Try(t)))
        .recover { case th => (e, Try(throw th)) }
      ).runWith(Sink.foreach {
      case (oldJob, res) =>
        self ! onJobStatus(oldJob, res)
    }).onComplete {
      case Success(d) =>
        self ! FinishedStatusCheckForAll
        Logger.info("Completed status check for all jobs")
      case Failure(e) =>
        self ! FinishedStatusCheckForAll
        Logger.error(s"Failed status check for all jobs. ${e.getMessage}", e)
    }
  }

  private def onJobStatus(oldProfilerJob: ProfilerJob, res: Try[ProfilerJob]): JobManagerMessage = {
    res match {
      case Success(job) =>
        if (oldProfilerJob.status != job.status || ProfilerJob.isCompleted(job.status)) {
          updateAssetRunStatus(job.id.get, job.status)
        }
        updateJob(job)
      case Failure(e) =>
        Logger.warn(s"Got error. Removing job ${oldProfilerJob.id.get}.", e)
        assetJobHistoryRepo.updateJobStatus(oldProfilerJob.id.get, JobStatus.UNKNOWN)
        RemoveManageJob(oldProfilerJob.id.get)
    }
  }

  private def updateJob(job: ProfilerJob) = {
    if (ProfilerJob.isCompleted(job.status)) {
        cacheRefreshHandler.clearCache() onComplete {
          case Success(_) => Logger.debug("Refreshed spark and in-memory cache")
          case Failure(error) => Logger.error("Failed to refresh cached tables and queries", error)
        }
      Logger.info(s"Job with id ${job.id.get} finished with status: ${job.status}. Removing from monitoring.")
      updateAssetRunStatus(job.id.get, job.status)
      RemoveManageJob(job.id.get)
    } else {
      ManageJob(job)
    }
  }

  private def updateAssetRunStatus(jobId: Long, jobStatus: JobStatus) = {
    assetJobHistoryRepo.updateJobStatus(jobId, jobStatus)
      .onComplete {
        case Success(no) =>
          Logger.info(s"Succefully updated asset run history for job ${jobId}. Affeted rows : ${no}")
        case Failure(e) =>
          Logger.error(s"Failed to update asset run history for job ${jobId}", e)
      }
  }

  private def saveAssetHistory(task: JobTask, job: ProfilerJob) = {
    assetJobHistoryRepo.saveAll(task.assets, job)
      .onComplete {
        case Success(r) =>
          Logger.info(s"Successfully saved history for ${job.id.get}")
        case Failure(e) =>
          Logger.info(s"Failed to save history for ${job.id.get}", e)
      }
  }

  override def receive: Receive = {
    case ScheduleAndManageJob(task, profiler) =>
      val originalSender = sender()
      val jF = jobService.startJob(task)
      jF.pipeTo(originalSender)
      jF.map {
        j =>
          saveAssetHistory(task, j)
          ManageJob(j)
      }.pipeTo(self)
    case ManageJob(job) => managedJobs.put(job.id.get, job)
    case RemoveManageJob(jobId) => managedJobs.remove(jobId)
    case CheckStatusForAll =>
      if (statusCheckInProgress) {
        Logger.info("Job Status Check is already in progress. Skipping now")
      } else {
        statusCheckInProgress = true
        checkStatusForAll()
      }
    case FinishedStatusCheckForAll =>
      Logger.info("Finished status check for all job")
      statusCheckInProgress = false
    case CheckStatus(jobId) =>
      jobService.jobStatus(jobId).pipeTo(sender)
    case KillJob(jobId) =>
      jobService.killJob(jobId).pipeTo(sender)
    case Failure(f) =>
      Logger.error(s"One of the operations resulted in a failure - ${f}")
    case a@_ => Logger.warn(s"Unknown message ${a}")
  }
}

abstract sealed class JobManagerMessage

case class ScheduleAndManageJob(jobTask: JobTask, profiler: ProfilerAndProfilerInstance) extends JobManagerMessage

case class ManageJob(job: ProfilerJob) extends JobManagerMessage

case class RemoveManageJob(jobId: Long) extends JobManagerMessage

case class CheckStatusForAll() extends JobManagerMessage

case class FinishedStatusCheckForAll() extends JobManagerMessage

case class CheckStatus(jobId: Long) extends JobManagerMessage

case class KillJob(jobId: Long) extends JobManagerMessage