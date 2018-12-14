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

package selector.source

import domain.{Asset, AssetType}
import job.selector.source.QueueAssetSource
import org.scalatest.mockito.MockitoSugar
import org.scalatestplus.play.PlaySpec
import play.api.libs.json.Json
import play.api.test.{DefaultAwaitTimeout, FutureAwaits}

class QueueAssetSourceSpec extends PlaySpec with MockitoSugar with FutureAwaits with DefaultAwaitTimeout {

  "Queue Asset Source" must {

    "get all assets in first batch if batch size is greater than queue size" in {
      val source = getQueueAssetSource(8, 4)

      val notDone = await(source.isDone())
      notDone mustBe false

      val assets = await(source.fetchNext())
      assets.length mustBe 4

      val done = await(source.isDone())
      done mustBe true

    }

    "get assets in multiple batch if batch size is less that queue size " in {
      val source = getQueueAssetSource(8, 15)

      val notDone = await(source.isDone())
      notDone mustBe false

      val assets = await(source.fetchNext())
      assets.length mustBe 8

      val notDone2 = await(source.isDone())
      notDone2 mustBe false

      val assets2 = await(source.fetchNext())
      assets2.length mustBe 7

      val done = await(source.isDone())
      done mustBe true
    }

    "return zero asset if queue is empty" in {
      val source = getQueueAssetSource(8, 0)

      val done = await(source.isDone())
      done mustBe true

      val assets = await(source.fetchNext())
      assets.isEmpty mustBe true
    }

    "return  future if we try to fetch from empty queue" in {
      val source = getQueueAssetSource(8, 8)

      val notDone = await(source.isDone())
      notDone mustBe false

      val assets = await(source.fetchNext())
      assets.length mustBe 8

      val done = await(source.isDone())
      done mustBe true

      val assets2 = await(source.fetchNext())
      assets2.isEmpty mustBe true
    }

  }

  def getQueueAssetSource(size: Int, assetSize: Int) = {
    import domain.JsonFormatters._
    val assets = getAssets(assetSize)
    val config = Json.obj(
      "size" -> size,
      "assets" -> Json.toJson(assets)
    )
    new QueueAssetSource(config)
  }

  def getAssets(elem: Int) = (0 until elem).map(i => new Asset(i.toString, AssetType.Hive, Json.obj()))

}
