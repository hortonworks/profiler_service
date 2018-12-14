/*
 HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES


  (c) 2016-2018 Hortonworks, Inc. All rights reserved.

  This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
  Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
  to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
  properly licensed third party, you do not have any rights to this code.

  If this code is provided to you under the terms of the AGPLv3:
  (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
  (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
    LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
  (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
    FROM OR RELATED TO THE CODE; AND
  (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
    DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
    DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
    OR LOSS OR CORRUPTION OF DATA.
*/
package job.incremental

import java.util
import javax.inject.{Inject, Singleton}

import commons.ApplicationMode
import job.incremental.source.HiveClientChangeEventSource
import job.scheduler.Scheduler
import org.quartz.{CronScheduleBuilder, JobBuilder, JobDataMap, TriggerBuilder}
import play.api.{Configuration, Logger}
import repo.ChangeNotificationLogRepo

@Singleton
class ChangeDetectorScheduler @Inject()(configuration: Configuration, changeNotificationLogRepo: ChangeNotificationLogRepo) {

  val logger = Logger(classOf[ChangeDetectorScheduler])
  private val scheduler = Scheduler.getScheduler
  private val startChangeDetectorCron = configuration.getString("dpprofiler.incremental.changedetector.cron").getOrElse("0 0 0/1 1/1 * ? *")
  val keepRangeMs = (configuration.getLong("dpprofiler.incremental.changedetector.keeprange").getOrElse(86400L))*1000
  val purgeOldLogs = (configuration.getBoolean("dpprofiler.incremental.changedetector.purge.oldlogs").getOrElse(true))
  val isLocalMode = ApplicationMode.isLocalMode(configuration)
  if(!isLocalMode) startChangeDetector //needed as this method will start throwing error in local if it does not find hive-site.xml

  private def startChangeDetector = {

    logger.info("Scheduling cron job to detect change in hive tables.")

    val triggerName = "trigger-name-increment"
    val triggerGroup = "trigger-group-increment"
    val trigger = TriggerBuilder.newTrigger()
      .withIdentity(triggerName, triggerGroup)
      .withSchedule(CronScheduleBuilder.cronSchedule(startChangeDetectorCron))
      .build()

    val jobName = "job-name-increment"
    val jobGroup = "job-group-increment"
    val hiveClient = HiveClientChangeEventSource(configuration, changeNotificationLogRepo)

    ChangeDetectorJob.logChangeEvents(hiveClient, changeNotificationLogRepo, keepRangeMs, purgeOldLogs)

    val map = new util.HashMap[String, Any]()
    map.put("changeEventSource", hiveClient)
    map.put("changeNotificationLogRepo",changeNotificationLogRepo)
    map.put("keepRange",keepRangeMs)
    map.put("purgeOldLogs",purgeOldLogs)

    val job = JobBuilder.newJob(classOf[ChangeDetectorJob])
      .withIdentity(jobName, jobGroup)
      .usingJobData(new JobDataMap(map))
      .build()

    scheduler.scheduleJob(job, trigger)
  }
}
