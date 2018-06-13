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

package job.interactive

import java.util.concurrent.TimeUnit
import javax.inject.{Inject, Singleton}

import com.google.common.cache.{CacheBuilder, CacheLoader}
import domain.InteractiveQuery
import play.api.Configuration
import play.api.libs.json.JsValue

import scala.concurrent.{Await, ExecutionContext, Future}

@Singleton
class LivyQueryCache @Inject()(livyInteractiveRunner: LivyInteractiveRunner, configuration: Configuration) {

  val cacheEnabled: Boolean = configuration.getBoolean("metrics.cache.enabled").getOrElse(true)
  val expireMinutes: Int = configuration.getInt("metrics.cache.expire.minutes").getOrElse(120)
  val maxCacheSize: Int = configuration.getInt("metrics.cache.size").getOrElse(5000)
  val queryTimeout: Int = configuration.getInt("metrics.cache.query.timeout.seconds").getOrElse(60)

  private val cacheOption =
    if (cacheEnabled) Some(buildCache())
    else None


  private def buildCache() = {
    CacheBuilder.newBuilder()
      .expireAfterWrite(expireMinutes, TimeUnit.MINUTES)
      .maximumSize(maxCacheSize)
      .build(new CacheLoader[InteractiveQuery, JsValue] {
        override def load(key: InteractiveQuery) = {
          import concurrent.duration._
          Await.result(livyInteractiveRunner.postInteractiveQueryAndGetResult(key), queryTimeout seconds)
        }
      })
  }

  def get(query: InteractiveQuery)(implicit ec: ExecutionContext) = {
    cacheOption match {
      case Some(cache) =>
        Future {
          cache.get(query)
        }
      case None => livyInteractiveRunner.postInteractiveQueryAndGetResult(query)

    }
  }

  def clearCache()(implicit ec: ExecutionContext): Future[Unit] = {
    Future {
      cacheOption.foreach(_.invalidateAll())
    }
  }

}
