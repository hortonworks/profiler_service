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

package job.profiler

import akka.actor.{Actor, Cancellable, PoisonPill}
import domain._
import job.manager.JobManager
import play.api.{Configuration, Logger}

import scala.concurrent.Future

class JobSubmitterActor(var profilerAndProfilerInstance: ProfilerAndProfilerInstance,
                        jobManager: JobManager,
                        config: Configuration) extends Actor {

  import JobSubmitterActor._

  private val batchSize = config.getInt("submitter.batch.size").getOrElse(20)
  private val maxQueueSize = config.getInt("submitter.queue.size").getOrElse(500)
  private val maxJobs = config.getInt("submitter.jobs.max").getOrElse(2)
  private val scanInterval = config.getInt("submitter.jobs.scan.seconds").getOrElse(30)
  private val scanInitialDelay = config.getInt("submitter.jobs.scan.delay.seconds").getOrElse(5)

  // TODO Uniqueness of element and priority queue
  private var queue = collection.mutable.PriorityQueue[AssetEnvelop]()

  private val currentJobs = collection.mutable.Map[Long, ProfilerJob]()

  var stopProcessing = false

  var scheduler: Cancellable = _

  import concurrent.duration._
  import scala.concurrent.ExecutionContext.Implicits.global

  startScheduler(scanInitialDelay seconds)

  override def receive = {
    case ScheduleJob =>
      Logger.info(s"Checking for schedule jobs for ${profilerAndProfilerInstance.profilerInstance.name}")
      processQueue

    case AddAsset(envelops) =>
      if (stopProcessing) {
        sender ! AssetRejected(envelops, "Submitter stopped")
      } else addAndProcessEnvelop(envelops)

    case UpdateProfilerInstance(profiler) =>
      profilerAndProfilerInstance = profiler

    case StopProfiler =>
      stopProcessing = true

    case _ =>
  }


  private def currentJobStatus() = {
    Future.sequence(currentJobs.keySet.map(jobManager.status))
  }

  // TODO Currently only checking number Of jobs.
  // Latter can check cluster resource etc. Pluggable rules
  private def canScheduleJob(jobs: Seq[ProfilerJob]): Future[Boolean] = {
    Future.successful(currentJobs.size < maxJobs)
  }

  private def scheduleJob(canSchedule: Boolean): Future[Option[ProfilerJob]] = {
    if (canSchedule && queue.nonEmpty) {
      val processSize = Math.min(batchSize, queue.size)
      val assetsToSchedule = (0 until processSize).map(_ => queue.dequeue().asset)

      scheduleJob(assetsToSchedule)
        .map(j => Some(j))
    } else if (stopProcessing && queue.isEmpty) {
      Logger.info(s"Sending poison pill for submitter ${profilerAndProfilerInstance.profilerInstance.name}")
      self ! PoisonPill
      Future.successful(None)
    } else Future.successful(None)

  }

  private def scheduleJob(assets: Seq[Asset]): Future[ProfilerJob] = {
    Logger.info("Scheduling job for ")
    val jobTask = JobTask(profilerAndProfilerInstance.profilerInstance.name, assets)
    jobManager.schedule(jobTask)
  }

  private def updateJobs(jobs: Seq[ProfilerJob]) = {
    jobs.foreach(j => currentJobs.put(j.id.get, j))
    currentJobs.values.filter(j => ProfilerJob.isCompleted(j.status)).toSeq
      .foreach(j => currentJobs.remove(j.id.get))
  }

  private def addAndProcessEnvelop(envelops: Seq[AssetEnvelop]) = {

    val sortedEnvelopes = envelops.sortBy(-_.priority)
    val acceptedEnvelops = collection.mutable.ArrayBuffer[AssetEnvelop]()
    val rejectedEnvelops = collection.mutable.ArrayBuffer[AssetEnvelop]()


    def processEnvelops = (envelop: AssetEnvelop) => {
      val isUnique = isUniqueInQueue(envelop)

      if (!isUnique) {
        val (q, isReplaced) = replaceIfHigherPriority(envelop)
        queue = q
        acceptedEnvelops += envelop
      } else if (queue.size < maxQueueSize) {
        queue += envelop
        acceptedEnvelops += envelop
      } else {
        rejectedEnvelops += envelop
      }
    }

    sortedEnvelopes.foreach(processEnvelops)

    if (acceptedEnvelops.nonEmpty && rejectedEnvelops.nonEmpty) {
      sender ! PartialRejected(rejectedEnvelops, s"Queue size is full. Some assets are accepted. Current Q size: ${queue.size}")
    } else if (acceptedEnvelops.isEmpty) {
      sender ! AssetRejected(rejectedEnvelops, s"Queue size full. Current Q size : ${queue.size}")
    } else {
      sender ! AssetAccepted()
    }

    if (queue.size >= maxQueueSize) {
      cancelCurrentScheduler
      processQueue
      startScheduler(scanInitialDelay seconds)
    }
  }

  private def processQueue = {
    (for {
      jobs <- currentJobStatus()
      _ = updateJobs(jobs.toSeq)
      canSchedule <- canScheduleJob(jobs.toSeq)
      jobOpt <- scheduleJob(canSchedule)
    } yield {
      jobOpt.map(j => currentJobs.put(j.id.get, j))
    }).recover {
      case e: Throwable =>
        Logger.error(e.getMessage, e)
    }
  }

  private def isUniqueInQueue(envelop: AssetEnvelop) = !queue.exists(x => x.asset.id == envelop.asset.id)

  private def replaceIfHigherPriority(envelop: AssetEnvelop) = {
    val accumulator = collection.mutable.PriorityQueue[AssetEnvelop]()
    queue.foldLeft((accumulator, false)) { (acc, item) =>
      val accumulator = acc._1
      if (item.asset.id == envelop.asset.id && envelop.priority > item.priority) {
        (accumulator += envelop, true)
      } else {
        (accumulator += item, false)
      }
    }
  }

  private def startScheduler(initialDelay: FiniteDuration): Unit = {
    scheduler = context.system.scheduler.schedule(initialDelay, scanInterval seconds, self, ScheduleJob)
  }

  private def cancelCurrentScheduler: Boolean = scheduler.cancel()

}


object JobSubmitterActor {

  class JobSubmitterMessage

  case class ScheduleJob() extends JobSubmitterMessage

  case class AddAsset(assets: Seq[AssetEnvelop]) extends JobSubmitterMessage

  case class AssetAccepted() extends JobSubmitterMessage

  case class AssetRejected(assetEnvelops: Seq[AssetEnvelop], reason: String) extends JobSubmitterMessage

  case class PartialRejected(assetEnvelops: Seq[AssetEnvelop], reason: String) extends JobSubmitterMessage

  case class UpdateProfilerInstance(profiler: ProfilerAndProfilerInstance) extends JobSubmitterMessage

  case class StopProfiler()

  def assetEnvelopOrder(envelop: AssetEnvelop): Int = envelop.priority // TODO: change this to weight which includes the createdAt
  implicit val assetEnvelopOrdering: Ordering[AssetEnvelop] = Ordering.by(assetEnvelopOrder)

}