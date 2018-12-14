/*
 * HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
 * (c) 2016-2018 Hortonworks, Inc. All rights reserved.
 * This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
 * Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
 * to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
 * properly licensed third party, you do not have any rights to this code.
 * If this code is provided to you under the terms of the AGPLv3:
 * (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
 * (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
 *   LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
 * (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
 *   FROM OR RELATED TO THE CODE; AND
 * (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
 *   DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
 *   DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
 *   OR LOSS OR CORRUPTION OF DATA.
 */

package selector.filters


import domain.{Asset, AssetJobHistory, AssetType, JobStatus}
import job.selector.filters.LastRunFilter
import org.joda.time.DateTime
import org.scalatest.mockito.MockitoSugar
import org.scalatestplus.play.PlaySpec
import play.api.libs.json.Json
import play.api.test.{DefaultAwaitTimeout, FutureAwaits}
import repo.AssetJobHistoryRepo
import org.mockito.Mockito._

import scala.concurrent.Future

class LastRunFilterSpec extends PlaySpec with MockitoSugar with FutureAwaits with DefaultAwaitTimeout {

  "LastRun Filter" must {

    val twoDaysInSeconds = 2 * 24 * 60 * 60
    val profilerInstanceName = "hive"
    val asset = new Asset("dummy", AssetType.Hive, Json.obj())

    "return false if no jobs exists for the asset" in {
      val assetJobHistoryRepo = mock[AssetJobHistoryRepo]
      val lastRunFilter = new LastRunFilter(assetJobHistoryRepo, profilerInstanceName, twoDaysInSeconds)

      when(assetJobHistoryRepo.getLastJob(asset.id, profilerInstanceName)).thenReturn(Future.successful(None))

      val result = await(lastRunFilter.evaluate(asset))

      verify(assetJobHistoryRepo, times(1)).getLastJob(asset.id, profilerInstanceName)
      result mustBe false
    }

    "return false if job completed successfully before 2 days" in {

      val assetJobHistoryRepo = mock[AssetJobHistoryRepo]
      val lastRunFilter = new LastRunFilter(assetJobHistoryRepo, profilerInstanceName, twoDaysInSeconds)

      val job = AssetJobHistory(None, profilerInstanceName, asset.id, 1, JobStatus.SUCCESS, DateTime.now().minusMillis(twoDaysInSeconds * 1000).minusMillis(100))
      when(assetJobHistoryRepo.getLastJob(asset.id, profilerInstanceName)).thenReturn(Future.successful(Some(job)))

      val result = await(lastRunFilter.evaluate(asset))

      verify(assetJobHistoryRepo, times(1)).getLastJob(asset.id, profilerInstanceName)
      result mustBe false

    }


    "return false if job is in failed status" in {
      val assetJobHistoryRepo = mock[AssetJobHistoryRepo]
      val lastRunFilter = new LastRunFilter(assetJobHistoryRepo, profilerInstanceName, twoDaysInSeconds)

      val job = AssetJobHistory(None, profilerInstanceName, asset.id, 1, JobStatus.FAILED, DateTime.now().minusMillis(100))
      when(assetJobHistoryRepo.getLastJob(asset.id, profilerInstanceName)).thenReturn(Future.successful(Some(job)))

      val result = await(lastRunFilter.evaluate(asset))

      verify(assetJobHistoryRepo, times(1)).getLastJob(asset.id, profilerInstanceName)
      result mustBe false
    }

    "return true if job is in running status" in {
      val assetJobHistoryRepo = mock[AssetJobHistoryRepo]
      val lastRunFilter = new LastRunFilter(assetJobHistoryRepo, profilerInstanceName, twoDaysInSeconds)

      val job = AssetJobHistory(None, profilerInstanceName, asset.id, 1, JobStatus.RUNNING, DateTime.now().minusMillis(100))
      when(assetJobHistoryRepo.getLastJob(asset.id, profilerInstanceName)).thenReturn(Future.successful(Some(job)))

      val result = await(lastRunFilter.evaluate(asset))

      verify(assetJobHistoryRepo, times(1)).getLastJob(asset.id, profilerInstanceName)
      result mustBe true


    }

    "return true if job is in started status" in {

      val assetJobHistoryRepo = mock[AssetJobHistoryRepo]
      val lastRunFilter = new LastRunFilter(assetJobHistoryRepo, profilerInstanceName, twoDaysInSeconds)

      val job = AssetJobHistory(None, profilerInstanceName, asset.id, 1, JobStatus.STARTED, DateTime.now().minusMillis(100))
      when(assetJobHistoryRepo.getLastJob(asset.id, profilerInstanceName)).thenReturn(Future.successful(Some(job)))

      val result = await(lastRunFilter.evaluate(asset))

      verify(assetJobHistoryRepo, times(1)).getLastJob(asset.id, profilerInstanceName)
      result mustBe true
    }

    "return true if job is success within two days" in {
      val assetJobHistoryRepo = mock[AssetJobHistoryRepo]
      val lastRunFilter = new LastRunFilter(assetJobHistoryRepo, profilerInstanceName, twoDaysInSeconds)

      val job = AssetJobHistory(None, profilerInstanceName, asset.id, 1,
        JobStatus.SUCCESS, DateTime.now().minusMillis(twoDaysInSeconds * 1000).plusMillis(2000))
      when(assetJobHistoryRepo.getLastJob(asset.id, profilerInstanceName)).thenReturn(Future.successful(Some(job)))

      val result = await(lastRunFilter.evaluate(asset))

      verify(assetJobHistoryRepo, times(1)).getLastJob(asset.id, profilerInstanceName)
      result mustBe true

      val job2 = AssetJobHistory(None, profilerInstanceName, asset.id, 1,
        JobStatus.SUCCESS, DateTime.now().minusMillis(twoDaysInSeconds * 1000).plusMillis(2000))
      when(assetJobHistoryRepo.getLastJob(asset.id, profilerInstanceName)).thenReturn(Future.successful(Some(job2)))

      val result2 = await(lastRunFilter.evaluate(asset))

      verify(assetJobHistoryRepo, times(2)).getLastJob(asset.id, profilerInstanceName)
      result2 mustBe true
    }

  }
}
