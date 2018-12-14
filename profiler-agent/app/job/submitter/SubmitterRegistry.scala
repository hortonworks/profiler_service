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

package job.submitter

import javax.inject.{Inject, Singleton}

import akka.actor.ActorSystem
import domain.{AssetSelectorDefinition, ProfilerAndProfilerInstance}
import job.manager.JobManager
import job.runner.ProfilerAgentException
import job.selector.AssetSelector
import job.selector.providers.{AssetFilterProvider, AssetSelectorProvider, AssetSourceProvider}
import play.api.{Configuration, Logger}
import repo.{AssetSelectorDefinitionRepo, ProfilerInstanceRepo}

@Singleton
private[submitter] class SubmitterRegistry @Inject()(profilerInstanceRepo: ProfilerInstanceRepo,
                                  jobManager: JobManager,
                                  sourceProvider: AssetSourceProvider,
                                  assetFilterProvider: AssetFilterProvider,
                                  assetSelectorDefinitionRepo: AssetSelectorDefinitionRepo,
                                  actorSystem: ActorSystem,
                                  config: Configuration) {

  import collection.convert.decorateAsScala._


  private val jobSubmitterMap = new java.util.concurrent.ConcurrentHashMap[String, JobSubmitter]().asScala


  def getSubmitterMap() = jobSubmitterMap.toMap

  def addProfilerInstance(profiler: ProfilerAndProfilerInstance) = {
    Logger.info(s"Initializing submitter for profiler : ${profiler}")
    jobSubmitterMap.put(
      profiler.profilerInstance.name,
      new JobSubmitter(profiler, jobManager, actorSystem, config)
    )
  }

  def updateProfilerInstance(profiler: ProfilerAndProfilerInstance) = {
    jobSubmitterMap.get(profiler.profilerInstance.name) match {
      case Some(submitter) => submitter.updateProfilerInstance(profiler)
      case None => throw new ProfilerAgentException(profiler.profilerInstance.name, s"No Profiler Instance found with Name : ${profiler.profilerInstance.name}")
    }
  }

  def stopProfilerInstance(profilerInstanceName: String) = {
    jobSubmitterMap.get(profilerInstanceName) match {
      case Some(submitter) =>
        Logger.info(s"Stopping submitter and selector for profiler : ${profilerInstanceName}")
        submitter.stop()
        jobSubmitterMap.remove(profilerInstanceName)
      case None => throw new ProfilerAgentException(profilerInstanceName, s"No Profiler Instance found with Name : ${profilerInstanceName}")
    }
  }

}
