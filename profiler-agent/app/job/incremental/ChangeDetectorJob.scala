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

import job.incremental.source.ChangeEventSource

import scala.concurrent._
import ExecutionContext.Implicits.global
import org.quartz.{Job, JobExecutionContext}
import play.api.Logger
import repo.ChangeNotificationLogRepo

import scala.concurrent.ExecutionContext

class ChangeDetectorJob extends Job {

  val logger = Logger(classOf[ChangeDetectorJob])

  override def execute(context: JobExecutionContext) = {

    logger.info("Executing change detector of incremental profiling")
    val changeEventSource = context.getMergedJobDataMap.get("changeEventSource").asInstanceOf[ChangeEventSource]
    val changeNotificationLogRepo = context.getMergedJobDataMap.get("changeNotificationLogRepo").asInstanceOf[ChangeNotificationLogRepo]
    val keepRange = context.getMergedJobDataMap.get("keepRange").asInstanceOf[Long]
    val purgeOldLogs = context.getMergedJobDataMap.get("purgeOldLogs").asInstanceOf[Boolean]
    ChangeDetectorJob.logChangeEvents(changeEventSource, changeNotificationLogRepo, keepRange, purgeOldLogs)
  }
}

object ChangeDetectorJob {

  val logger = Logger(classOf[ChangeDetectorJob])

  def logChangeEvents(changeEventSource: ChangeEventSource, changeNotificationLogRepo: ChangeNotificationLogRepo, keepRange: Long, purgeOldLogs: Boolean) = {

    logger.info(s"Logging all change events")

    changeEventSource.nextChangeLogs.flatMap { changeNotifications =>

      changeNotificationLogRepo.add(changeNotifications).map{ dc =>
        if (purgeOldLogs) purgeOldEntry(keepRange, changeNotificationLogRepo)
        LogAddStatus.SUCCESS
      }
    }
      .recover {
        case th: Throwable =>
          logger.error("Failed to log change events.", th)
          LogAddStatus.FAILURE
      }
  }

  private def purgeOldEntry(keepRange: Long, changeNotificationLogRepo: ChangeNotificationLogRepo) = {

    changeNotificationLogRepo.deleteOlder(keepRange).map { res =>
      logger.info("number of entries purged " + res)
    }
      .recover {
        case th: Throwable => logger.error("Delete older change notification log failed", th)
      }
  }
}

object  LogAddStatus extends Enumeration {
  type LogAddStatus = Value
  val SUCCESS, FAILURE = Value
}