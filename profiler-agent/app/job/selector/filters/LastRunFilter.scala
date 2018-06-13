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

package job.selector.filters

import domain.{Asset, JobStatus}
import job.selector.SelectorConstants
import org.joda.time.DateTime
import play.api.libs.json.JsObject
import repo.AssetJobHistoryRepo

/**
  * Returns true if asset is currently running or run within minTime
  * Use it as Drop filter
  *
  * @param assetJobHistoryRepo
  * @param profilerInstance
  * @param minTimeSec
  */
class LastRunFilter(assetJobHistoryRepo: AssetJobHistoryRepo,
                    profilerInstance: String, minTimeSec: Long) extends AssetFilter {

  import scala.concurrent.ExecutionContext.Implicits.global

  override def name = SelectorConstants.LastRunFilter

  private val completedOrRunningJob = Set(
    JobStatus.RUNNING,
    JobStatus.STARTED,
    JobStatus.SUCCESS
  )

  override def evaluate(asset: Asset) = {
    assetJobHistoryRepo.getLastJob(asset.id, profilerInstance).map {
      case Some(job) =>
        (completedOrRunningJob.contains(job.status) &&
          job.lastUpdated.getMillis > DateTime.now().getMillis - minTimeSec * 1000)
      case None => false
    }
  }
}

object LastRunFilter {

  def apply(assetJobHistoryRepo: AssetJobHistoryRepo, profilerInstance: String, config: JsObject) = {
    val oneDay: Long = 24 * 60 * 60
    val minTimeSec = (config \ "minTimeSec").asOpt[Long].getOrElse(oneDay)
    new LastRunFilter(assetJobHistoryRepo, profilerInstance, minTimeSec)
  }
}
