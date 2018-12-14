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

package job.selector.selectors

import java.util
import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.ActorSystem
import domain.{AssetSelectorDefinition, AssetSourceDefinition}
import job.runner.ProfilerAgentException
import job.scheduler.Scheduler
import job.selector.filters.AssetFilter
import job.selector.providers.{AssetFilterProvider, AssetSourceProvider}
import job.selector.{AssetSelector, AssetSource, PullAssetSource}
import job.submitter.AssetSubmitter
import org.quartz._
import play.api.{Configuration, Logger}
import repo.AssetSelectorDefinitionRepo

import scala.concurrent.Future
import scala.util.{Failure, Success}

class CronBasedSelector(
                         val name: String,
                         cronExpr: String,
                         scheduleOnStart: Boolean,
                         val profilerInstanceName: String,
                         val pickFilters: Seq[AssetFilter],
                         val dropFilters: Seq[AssetFilter],
                         val submitter: AssetSubmitter,
                         val sourceDefinition: AssetSourceDefinition,
                         val sourceProvider: AssetSourceProvider,
                         val system: ActorSystem,
                         val assetSelectorDefinitionRepo: AssetSelectorDefinitionRepo
                       ) extends AssetSelector {

  val scheduler = Scheduler.getScheduler()

  private val jobName = s"${name}-job"

  Logger.info(s"Scheduling selector ${name}. Cron : ${cronExpr}")


  private val group = s"selector-group"
  val trigger = TriggerBuilder.newTrigger()
    .withIdentity(s"${name}-trigger", group)
    .withSchedule(CronScheduleBuilder.cronSchedule(cronExpr))
    .build()

  private val running = new AtomicBoolean(false)

  private val jobData = new util.HashMap[String, Object]()
  jobData.put("cronSelector", this)
  jobData.put("running", running)

  if (!scheduler.checkExists(new JobKey(jobName, group))) {
    val job = JobBuilder.newJob(classOf[CronSelectorJob])
      .withIdentity(jobName, group)
      .usingJobData(new JobDataMap(jobData))
      .build()

    if (scheduleOnStart) {

      val job = JobBuilder.newJob(classOf[CronSelectorJob])
        .withIdentity(s"${jobName}-now", group)
        .usingJobData(new JobDataMap(jobData))
        .build()

      val nowTrigger = TriggerBuilder.newTrigger()
        .withIdentity(s"${name}-now-trigger", group)
        .startNow().build()
      scheduler.scheduleJob(job, nowTrigger)
    }

    scheduler.scheduleJob(job, trigger)

  }

  override def stop(): Unit = {
    scheduler.deleteJob(JobKey.jobKey(jobName, group))
  }
}

object CronBasedSelector {

  import scala.concurrent.ExecutionContext.Implicits.global

  def apply(assetSelectorDefinition: AssetSelectorDefinition,
            sourceProvider: AssetSourceProvider, assetFilterProvider: AssetFilterProvider,
            submitter: AssetSubmitter, system: ActorSystem, configuration: Configuration,
            assetSelectorDefinitionRepo: AssetSelectorDefinitionRepo): Future[CronBasedSelector] = {

    val cronExpr = (assetSelectorDefinition.config \ "cronExpr").as[String]
    val scheduleOnStart = (assetSelectorDefinition.config \ "scheduleOnStart").asOpt[Boolean].getOrElse(false)
    val pickFiltersFuture = Future.sequence(
      assetSelectorDefinition.pickFilters.map(e => assetFilterProvider.get(
        assetSelectorDefinition.profilerInstanceName, e
      ))
    )
    val dropFiltersFuture = Future.sequence(
      assetSelectorDefinition.dropFilters.map(e => assetFilterProvider.get(
        assetSelectorDefinition.profilerInstanceName, e
      ))
    )

    for {
      pickFilters <- pickFiltersFuture
      dropFilters <- dropFiltersFuture
    } yield {
      new CronBasedSelector(
        assetSelectorDefinition.name,
        cronExpr,
        scheduleOnStart,
        assetSelectorDefinition.profilerInstanceName,
        pickFilters,
        dropFilters,
        submitter,
        assetSelectorDefinition.sourceDefinition,
        sourceProvider,
        system,
        assetSelectorDefinitionRepo
      )
    }
  }
}


class CronSelectorJob extends Job {

  private def checkSourceValidity(cronSelector: CronBasedSelector, source: AssetSource): Future[Boolean] = {
    if (!source.isInstanceOf[PullAssetSource]) {
      val msg = s"Invalid source for ${cronSelector.name} selector. Source : ${cronSelector.sourceDefinition} ." +
        s"Cron Selector only Supports Pull based Source"
      Logger.error(msg)
      Future.failed(new ProfilerAgentException("", msg))
    } else Future.successful(true)
  }

  override def execute(context: JobExecutionContext): Unit = {

    import scala.concurrent.ExecutionContext.Implicits.global

    context.getJobDetail

    val cronSelector = context.getMergedJobDataMap().get("cronSelector").asInstanceOf[CronBasedSelector]
    val running = context.getMergedJobDataMap().get("running").asInstanceOf[AtomicBoolean]

    if (running.get()) {
      Logger.warn(s"Previous job is still running for selector : ${cronSelector.name}. Skipping this job")
      return
    }

    Logger.info(s"Running job for selector : ${cronSelector.name}")

    def runSelector(source: AssetSource) = {
      running.set(true)

      val pullSelector = new PullSourceAssetSelector(
        cronSelector.name,
        cronSelector.profilerInstanceName,
        cronSelector.pickFilters,
        cronSelector.dropFilters,
        cronSelector.submitter,
        source.asInstanceOf[PullAssetSource],
        cronSelector.system,
        assetSelectorDefinitionRepo = cronSelector.assetSelectorDefinitionRepo
      )

      pullSelector.execute()
    }

    val jobFuture = (for (
      source <- cronSelector.sourceProvider.get(cronSelector.sourceDefinition, cronSelector.name);
      _ <- checkSourceValidity(cronSelector, source);
      exitCode <- runSelector(source)
    ) yield exitCode)


    jobFuture.onComplete {
      case Success(res) =>
        running.set(false)
        Logger.info(s"Selector job for ${cronSelector.name} finished successfully")
      case Failure(th) =>
        running.set(false)
        Logger.error(s"Selector job : ${cronSelector.name} stop due to error.", th)
    }
  }
}