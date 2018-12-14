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

import domain.{Asset, AssetType}
import job.selector.filters.InAssetCollectionFilter
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatestplus.play.PlaySpec
import play.api.libs.json.Json
import play.api.test.{DefaultAwaitTimeout, FutureAwaits}
import repo.DatasetRepo

import scala.concurrent.Future


class InAssetCollectionFilterSpec extends PlaySpec with MockitoSugar with FutureAwaits with DefaultAwaitTimeout {

  "InAssetCollection Filter" must {
    val asset = new Asset("dummy", AssetType.Hive, Json.obj())

    "return true if asset is in asset collection" in {
      val mockRepo = mock[DatasetRepo]
      val inAssetCollectionFilter = new InAssetCollectionFilter(mockRepo)
      when(mockRepo.assetExists("dummy")) thenReturn Future.successful(true)
      val result = await(inAssetCollectionFilter.evaluate(asset))

      verify(mockRepo, times(1)).assetExists("dummy")
      result mustBe true
    }

    "return false if asset is not in asset collection" in {
      val mockRepo = mock[DatasetRepo]
      val inAssetCollectionFilter = new InAssetCollectionFilter(mockRepo)
      when(mockRepo.assetExists("dummy")) thenReturn Future.successful(false)
      val result = await(inAssetCollectionFilter.evaluate(asset))

      verify(mockRepo, times(1)).assetExists("dummy")
      result mustBe false
    }
  }

}
