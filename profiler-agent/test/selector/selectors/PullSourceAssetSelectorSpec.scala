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

package selector.selectors

import java.time.LocalDateTime

import akka.actor.ActorSystem
import domain.{Asset, AssetEnvelop}
import job.profiler.JobSubmitterActor.{AssetAccepted, AssetRejected, PartialRejected}
import job.selector.PullAssetSource
import job.selector.selectors.PullSourceAssetSelector
import job.submitter.AssetSubmitter
import repo.AssetSelectorDefinitionRepo
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatestplus.play.PlaySpec
import play.api.test.{DefaultAwaitTimeout, FutureAwaits}

import scala.concurrent.{Await, Future}


class PullSourceAssetSelectorSpec extends PlaySpec with MockitoSugar with FutureAwaits with DefaultAwaitTimeout {

  "Pull Source Selector" must {

    val profilerInstanceName = "profiler1"
    val name = "profiler1-selector"

    import scala.concurrent.ExecutionContext.Implicits.global

    "should submit all data when submitter queue is empty" in {
      val submitter = mock[AssetSubmitter]
      val source = mock[PullAssetSource]
      val assetSelector = mock[AssetSelectorDefinitionRepo]
      val system = ActorSystem()

      val assets = Seq(mock[Asset], mock[Asset])

      val selector = new PullSourceAssetSelector(name, profilerInstanceName, Nil, Nil, submitter, source, system, 1, assetSelector)
      val selectorSpy = Mockito.spy(selector)

      when(source.fetchNext())
        .thenReturn(Future.successful(assets))
        .thenReturn(Future.successful(Nil))

      when(source.isDone())
        .thenReturn(Future.successful(false))
        .thenReturn(Future.successful(true))

      // when submitter queue is empty, submitAssets will always return AssetAccepted()
      // using doReturn as using when() on spy object call real method
      doReturn(
        Future.successful(AssetAccepted()), null
      ).when(selectorSpy).submitAssets(assets)

      doReturn(
        Future.successful(AssetAccepted()), null
      ).when(selectorSpy).submitAssets(Nil)

      await(selectorSpy.execute())

      verify(source, times(2)).fetchNext()
      verify(source, times(2)).isDone()
      verify(selectorSpy, times(1)).submitAssets(assets)
      verify(selectorSpy, times(1)).submitAssets(Nil)
    }

    "should submit all data when submitter queue is partially filled" in {
      val submitter = mock[AssetSubmitter]
      val source = mock[PullAssetSource]
      val assetSelector = mock[AssetSelectorDefinitionRepo]
      val system = ActorSystem()

      val asset1 = mock[Asset]
      val asset2 = mock[Asset]
      val assets = Seq(asset1, asset2)
      val assets2 = Seq(asset2)

      val selector = new PullSourceAssetSelector(name, profilerInstanceName, Nil, Nil, submitter, source, system, 1, assetSelector)
      val selectorSpy = Mockito.spy(selector)

      when(source.fetchNext()).thenReturn(Future.successful(assets))
      when(source.isDone())
        .thenReturn(Future.successful(true))

      // when submitter queue is partially filled, submitAssets will return PartialRejected
      doReturn(
        Future.successful(PartialRejected(Seq(AssetEnvelop(asset2, 1, LocalDateTime.now())), "asset2 rejected")), null
      ).when(selectorSpy).submitAssets(assets)

      doReturn(
        Future.successful(AssetRejected(Seq(AssetEnvelop(asset2, 1, LocalDateTime.now())), "asset2 rejected")),
        Future.successful(AssetAccepted())
      ).when(selectorSpy).submitAssets(assets2)

      val future = selectorSpy.execute()

      val start = System.currentTimeMillis()
      import concurrent.duration._
      Await.result(future, 4 seconds)
      val timeTaken = System.currentTimeMillis() - start

      timeTaken > 2000 mustBe true

      verify(source, times(1)).fetchNext()
      verify(source, times(1)).isDone()
      verify(selectorSpy, times(1)).submitAssets(assets)
      verify(selectorSpy, times(2)).submitAssets(assets2)
    }

    "should submit all data when submitter queue is fully filled" in {
      val submitter = mock[AssetSubmitter]
      val source = mock[PullAssetSource]
      val assetSelector = mock[AssetSelectorDefinitionRepo]
      val system = ActorSystem()

      val asset1 = mock[Asset]
      val asset2 = mock[Asset]
      val assets2 = Seq(asset2)

      val selector = new PullSourceAssetSelector(name, profilerInstanceName, Nil, Nil, submitter, source, system, 1, assetSelector)
      val selectorSpy = Mockito.spy(selector)

      when(source.fetchNext()).thenReturn(Future.successful(assets2))
      when(source.isDone())
        .thenReturn(Future.successful(true))

      // when submitter queue is filled, submitAssets will return AssetRejected

      doReturn(
        Future.successful(AssetRejected(Seq(AssetEnvelop(asset2, 1, LocalDateTime.now())), "asset2 rejected")),
        Future.successful(AssetRejected(Seq(AssetEnvelop(asset2, 1, LocalDateTime.now())), "asset2 rejected")),
        Future.successful(AssetRejected(Seq(AssetEnvelop(asset2, 1, LocalDateTime.now())), "asset2 rejected")),
        Future.successful(AssetAccepted())
      ).when(selectorSpy).submitAssets(assets2)

      val future = selectorSpy.execute()

      val start = System.currentTimeMillis()
      import concurrent.duration._
      Await.result(future, 5 seconds)
      val timeTaken = System.currentTimeMillis() - start

      timeTaken > 3000 mustBe true

      verify(source, times(1)).fetchNext()
      verify(source, times(1)).isDone()
      verify(selectorSpy, times(4)).submitAssets(assets2)
    }

  }

}
