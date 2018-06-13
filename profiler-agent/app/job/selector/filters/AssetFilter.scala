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

import domain.Asset
import play.api.Logger

import scala.concurrent.{ExecutionContext, Future}

trait AssetFilter {

  def name: String

  def evaluate(asset: Asset): Future[Boolean]
}


object AssetFilter {

  def applyFilters(dropFilters: Seq[AssetFilter], pickFilters: Seq[AssetFilter], assets: Seq[Asset])
                  (implicit ec: ExecutionContext): Future[Seq[Asset]] = {
    val filteredAssets: Future[Seq[Option[Asset]]] = Future.sequence(assets.map(a => applyFiltersToAsset(dropFilters, pickFilters, a)))
    filteredAssets.map(_.filter(_.isDefined).map(_.get))
  }

  def applyFiltersToAsset(dropFilters: Seq[AssetFilter], pickFilters: Seq[AssetFilter], asset: Asset)
                         (implicit executionContext: ExecutionContext): Future[Option[Asset]] = {
    applyFiltersToAsset(dropFilters, asset).flatMap {
      res =>
        if (res) Future.successful(None)
        else {
          AssetFilter.applyFiltersToAsset(pickFilters, asset).map {
            pickRes =>
              if (pickRes) Some(asset)
              else None
          }
        }
    }
  }

  def applyFiltersToAsset(filters: Seq[AssetFilter], asset: Asset)(implicit ec: ExecutionContext): Future[Boolean] = {
    filters match {
      case head :: tail =>
        head.evaluate(asset).flatMap { value =>
          if (value) {
            Logger.debug(s"Filter '${head.name}' evaluates true for asset ${asset.id}")
            Future.successful(value)
          } else applyFiltersToAsset(tail, asset)
        }
      case Nil => Future.successful(false)
    }
  }
}