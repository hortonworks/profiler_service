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

package submitter

import akka.actor.ActorSystem
import domain.{Asset, AssetEnvelop, JobStatus, JobTask, ProfilerAndProfilerInstance, ProfilerInstance, ProfilerJob}
import job.manager.JobManager
import job.profiler.JobSubmitterActor.{AssetAccepted, AssetRejected, PartialRejected}
import job.submitter.JobSubmitter
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import org.scalatest.{BeforeAndAfter, BeforeAndAfterEach}
import org.scalatest.mockito.MockitoSugar
import org.scalatestplus.play.PlaySpec
import play.api.Configuration
import play.api.test.{DefaultAwaitTimeout, FutureAwaits}

import scala.concurrent.Future

class JobSubmitterSpec extends PlaySpec with MockitoSugar with FutureAwaits with DefaultAwaitTimeout with BeforeAndAfterEach {

  var actorSystem: ActorSystem = _

  override def beforeEach() = {
    actorSystem = ActorSystem()
  }

  override def afterEach() = {
    actorSystem.terminate()
  }

  "Job Submitter with empty queue" must {

    "submit all assets" in {

      val assets = getAssetEnvelops(2)
      val submitter = getSubmitter()

      val result = await(submitter.submitAsset(assets))

      result mustBe AssetAccepted()

      verifyAssetEnvelopsForCall(assets)

    }

    "reject asset after stop" in {

      val assets = getAssetEnvelops(2)
      val submitter = getSubmitter()

      submitter.stop()

      val result = await(submitter.submitAsset(assets))
      result mustBe AssetRejected(assets, "Submitter stopped")

      verifyAssetEnvelopsForNoCall(assets)
    }
  }

  "Job Submitter with partially filled queue" must {

    "partially reject assets" in {

      val assets = getAssetEnvelops(15)
      val profiler = mock[ProfilerAndProfilerInstance]
      val jobManager = mock[JobManager]

      val profilerInstance = mock[ProfilerInstance]
      when(profilerInstance.name).thenReturn("test")
      when(profiler.profilerInstance).thenReturn(profilerInstance)

      val job = mock[ProfilerJob]
      when(job.status).thenReturn(JobStatus.RUNNING)
      when(job.id).thenReturn(Option(1l))

      when(jobManager.schedule(any(classOf[JobTask])))
        .thenReturn(Future.successful(job))

      when(jobManager.status(1l)).thenReturn(Future.successful(job))

      val submitter = getSubmitter(profiler = profiler, jobManager = jobManager)

      val result = await(submitter.submitAsset(assets))
      Thread.sleep(300)

      result.getClass mustBe classOf[PartialRejected]
      result.asInstanceOf[PartialRejected].assetEnvelops.size mustBe 5

      verify(profilerInstance, times(1)).name
      verify(jobManager, times(1)).schedule(any(classOf[JobTask]))

      verifyAssetEnvelopsForCall(assets)

    }

  }

  "Job Submitter with filled queue" must {

    "reject assets" in {

      val assets = getAssetEnvelops(10, "a")
      val assets2 = getAssetEnvelops(5, "b")
      val assets3 = getAssetEnvelops(5, "c")

      val profiler = mock[ProfilerAndProfilerInstance]
      val jobManager = mock[JobManager]

      val profilerInstance = mock[ProfilerInstance]
      when(profilerInstance.name).thenReturn("test")
      when(profiler.profilerInstance).thenReturn(profilerInstance)

      val job = mock[ProfilerJob]
      when(job.status).thenReturn(JobStatus.RUNNING)
      when(job.id).thenReturn(Option(1l))

      when(jobManager.schedule(any(classOf[JobTask])))
        .thenReturn(Future.successful(job))

      when(jobManager.status(1l)).thenReturn(Future.successful(job))

      val submitter = getSubmitter(profiler = profiler, jobManager = jobManager)

      val result = await(submitter.submitAsset(assets))
      Thread.sleep(200)
      result mustBe AssetAccepted()


      val result2 = await(submitter.submitAsset(assets2))
      Thread.sleep(200)
      result2.getClass mustBe classOf[PartialRejected]
      result2.asInstanceOf[PartialRejected].assetEnvelops.size mustBe 3

      val result3 = await(submitter.submitAsset(assets3))
      result3.getClass mustBe classOf[AssetRejected]
      result3.asInstanceOf[AssetRejected].assetEnvelops.size mustBe 5

      verify(profilerInstance, times(1)).name
      verify(jobManager, times(2)).status(1l)
      verify(jobManager, times(1)).schedule(any(classOf[JobTask]))

      verifyAssetEnvelopsForCall(assets)
      verifyAssetEnvelopsForCall(assets2)
      verifyAssetEnvelopsForCall(assets3)
    }
  }


  def getSubmitter(configuration: Configuration = getConfigurationMock(),
                   profiler: ProfilerAndProfilerInstance = mock[ProfilerAndProfilerInstance],
                   jobManager: JobManager = mock[JobManager]
                  ) = {
    new JobSubmitter(profiler, jobManager, actorSystem, configuration)
  }

  def getAssetEnvelops(no: Int, prefix: String = "z") = {
    (0 until no).map {
      i =>
        val ae = mock[AssetEnvelop]
        val asset = mock[Asset]
        when(asset.id).thenReturn(s"${prefix}_${i.toString}")
        when(ae.asset).thenReturn(asset)
        when(ae.priority).thenReturn(0)
        ae
    }.toSeq
  }

  def verifyAssetEnvelopsForCall(envelops: Seq[AssetEnvelop]) = {
    envelops.foreach {
      e =>
        verify(e, atLeastOnce()).priority
        verify(e, atLeastOnce()).asset
        verify(e.asset, atLeastOnce()).id
    }
  }

  def verifyAssetEnvelopsForNoCall(envelops: Seq[AssetEnvelop]) = {
    envelops.foreach {
      e =>
        verify(e, times(0)).priority
        verify(e, times(0)).asset
        verify(e.asset, times(0)).id
    }
  }

  def getConfigurationMock(batchSize: Int = 2, queueSize: Int = 10, maxJobs: Int = 1, scan: Int = 30, scanDelay: Int = 10) = {
    val configuration = mock[Configuration]

    when(configuration.getInt("submitter.batch.size")).thenReturn(Some(batchSize))
    when(configuration.getInt("submitter.queue.size")).thenReturn(Some(queueSize))
    when(configuration.getInt("submitter.jobs.max")).thenReturn(Some(maxJobs))
    when(configuration.getInt("submitter.jobs.scan.seconds")).thenReturn(Some(scan))
    when(configuration.getInt("submitter.jobs.scan.delay.seconds")).thenReturn(Some(scanDelay))

    configuration
  }

}
