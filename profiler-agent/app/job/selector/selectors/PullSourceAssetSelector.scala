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

import java.time.Instant

import akka.actor.ActorSystem
import job.profiler.JobSubmitterActor.{AssetAccepted, AssetRejected, JobSubmitterMessage, PartialRejected}
import job.selector.filters.AssetFilter
import job.selector.{AssetSelector, PullAssetSource}
import job.submitter.AssetSubmitter
import play.api.Logger
import repo.AssetSelectorDefinitionRepo

import scala.concurrent.Future

class PullSourceAssetSelector(
                               val name: String,
                               val profilerInstanceName: String,
                               val pickFilters: Seq[AssetFilter],
                               val dropFilters: Seq[AssetFilter],
                               val submitter: AssetSubmitter,
                               val source: PullAssetSource,
                               system: ActorSystem,
                               retrySeconds: Int = 60,
                               val assetSelectorDefinitionRepo: AssetSelectorDefinitionRepo
                             ) extends AssetSelector {

  import concurrent.duration._
  import scala.concurrent.ExecutionContext.Implicits.global

  private val logger = Logger(classOf[PullSourceAssetSelector])

  private def fetchAndSubmit() = {
    source.fetchNext().flatMap(e => submitAssets(e))
      .flatMap(e => onSubmitResponse(e))
  }

  def execute() = {
    fetchAndSubmit().map{ res =>
      if(res==1) {
        val currTimeInMs = Instant.now().toEpochMilli
        logger.info("successfully submitted asset. Now updating source last submitted time in DB for "+name)
        assetSelectorDefinitionRepo.updateSourceLastSubmitted(name, currTimeInMs)
      }
    }
  }


  private def onSubmitResponse(submitResult: JobSubmitterMessage): Future[Int] = {
    submitResult match {
      case accepted: AssetAccepted =>
        source.isDone().flatMap { isDone =>
          if (isDone) Future.successful(1)
          else fetchAndSubmit()
        }
      case partialRejected: PartialRejected =>
        akka.pattern.after(retrySeconds seconds, using = system.scheduler)(submitAssets(partialRejected.assetEnvelops.map(_.asset)).flatMap(onSubmitResponse))
      case rejected: AssetRejected =>
        akka.pattern.after(retrySeconds seconds, using = system.scheduler)(submitAssets(rejected.assetEnvelops.map(_.asset)).flatMap(onSubmitResponse))
    }
  }

  override def stop(): Unit = {}
}
