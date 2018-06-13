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

package selector.filters

import com.hortonworks.dataplane.profilers.commons.domain.AssetType
import domain.Asset
import job.selector.filters.{AllFilter, AssetFilter, NoneFilter}
import org.scalatestplus.play.PlaySpec
import play.api.libs.json.Json
import play.api.test.{DefaultAwaitTimeout, FutureAwaits}

import scala.concurrent.Future

class FilterSpec extends PlaySpec with FutureAwaits with DefaultAwaitTimeout{

  private val allFilter = new AllFilter
  private val noneFilter = new NoneFilter
  private val asset = new Asset("dummy", AssetType.Hive, Json.obj())
  private val asset2 = new Asset("a-dummy", AssetType.Hive, Json.obj())

  class NameStartsAFilter extends AssetFilter {

    override def name: String = "nsa"

    override def evaluate(asset: Asset): Future[Boolean] = Future.successful(asset.id.toLowerCase.startsWith("a"))
  }

  val nameStartsWithAFilter = new NameStartsAFilter()

  import scala.concurrent.ExecutionContext.Implicits.global

  "Drop filter" must {

    "drop asset when evaluates true" in {
      val assetOptionFuture = AssetFilter.applyFiltersToAsset(Seq(allFilter), Seq(), asset)
      val assetOption = await(assetOptionFuture)
      assetOption mustBe None
    }

    "drop asset when evaluates false and no pick filter evaluates true" in {
      val assetOptionFuture = AssetFilter.applyFiltersToAsset(Seq(noneFilter), Seq(), asset)
      val assetOption = await(assetOptionFuture)
      assetOption mustBe None
    }

    "drop asset when no filters are present" in {
      val assetOptionFuture = AssetFilter.applyFiltersToAsset(Seq(), Seq(), asset)
      val assetOption = await(assetOptionFuture)
      assetOption mustBe None
    }


  }

  "Pick Filter" must {
    "pick asset when evaluates true" in {
      val assetOptionFuture = AssetFilter.applyFiltersToAsset(Seq(), Seq(allFilter), asset)
      val assetOption = await(assetOptionFuture)
      assetOption mustBe Some(asset)
    }

    "drop asset when none pick filters evaluates true" in {
      val assetOptionFuture = AssetFilter.applyFiltersToAsset(Seq(), Seq(noneFilter), asset)
      val assetOption = await(assetOptionFuture)
      assetOption mustBe None
    }
  }

  "Pick And Drop Filter" must {
    val assets = Seq(asset,asset2)
    "pick asset if none drops and someone picks" in {
      val assetOptionFuture = AssetFilter.applyFiltersToAsset(Seq(noneFilter), Seq(noneFilter, allFilter), asset)
      val assetOption = await(assetOptionFuture)
      assetOption mustBe Some(asset)
    }

    "pick assets if none drops and someone picks 1" in {
      val assetOptionFuture = AssetFilter.applyFilters(Seq(noneFilter, nameStartsWithAFilter), Seq(noneFilter, allFilter), assets)
      val assetOption = await(assetOptionFuture)
      assetOption mustBe Seq(asset)
    }

    "pick assets if none drops and someone picks 2" in {
      val assetOptionFuture = AssetFilter.applyFilters(Seq(noneFilter), Seq(noneFilter, nameStartsWithAFilter), assets)
      val assetOption = await(assetOptionFuture)
      assetOption mustBe Seq(asset2)
    }

    "drop assets if someone drops" in {
      val assetOptionFuture = AssetFilter.applyFilters(Seq(noneFilter, allFilter), Seq(noneFilter, allFilter, nameStartsWithAFilter), assets)
      val assetOption = await(assetOptionFuture)
      assetOption mustBe Seq()
    }

    "drop asset if someone drops 2" in {
      val assetOptionFuture = AssetFilter.applyFiltersToAsset(Seq(noneFilter, allFilter), Seq(noneFilter, allFilter), asset)
      val assetOption = await(assetOptionFuture)
      assetOption mustBe None
    }

  }
}
