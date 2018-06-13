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

package job.submitter

import javax.inject.{Inject, Singleton}

import domain.AssetSelectorDefinition
import job.selector.AssetSelector
import job.selector.providers.AssetSelectorProvider
import play.api.Logger

@Singleton
private[submitter] class SelectorRegistry @Inject()(
                                  submitterRegistry: SubmitterRegistry,
                                  selectorProvider: AssetSelectorProvider,
                                  assetSubmitter: AssetSubmitter
                                ) {

  import collection.convert.decorateAsScala._
  import scala.concurrent.ExecutionContext.Implicits.global

  private val selectorMap = new java.util.concurrent.ConcurrentHashMap[String, AssetSelector]().asScala

  def addSelectorDefinition(selectorDefinition: AssetSelectorDefinition) = {
    submitterRegistry.getSubmitterMap().get(selectorDefinition.profilerInstanceName) match {
      case Some(submitter) =>
        val selector = selectorProvider.get(selectorDefinition)
          .map(s => selectorMap.put(s.name, s))
        Logger.info(s"Initializing selector ${selectorDefinition}")
      case None => Logger.info(s"No profiler instance found. Skipping selector ${selectorDefinition}.")
    }
  }


  def removeSelectorByName(name: String) = {
    selectorMap.get(name).map(_.stop())
    selectorMap.remove(name)
  }

  def removeSelectorByProfilerInstance(profilerInstaneName: String) = {

    val selectors = selectorMap.values
      .filter(_.profilerInstanceName == profilerInstaneName)

    val selectorNames = selectors.map(_.name)
    Logger.info(s"Stopping selectors : ${selectorNames.mkString(",")}")

    selectors.foreach(_.stop())

    selectorNames.map(selectorMap.remove)

  }
}
