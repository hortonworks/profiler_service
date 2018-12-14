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

package com.hortonworks.dataplane.profilers.asset.module

import akka.actor.ActorSystem
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import com.google.inject.{AbstractModule, Provides, Singleton}
import com.hortonworks.dataplane.profilers.asset.hive.atlas.AtlasKafkaConsumer
import com.hortonworks.dataplane.profilers.asset.http.routes.DiscoverAssetRoute
import com.hortonworks.dataplane.profilers.commons.client.ProfilerAgentClient
import com.typesafe.config.{Config, ConfigFactory}
import play.api.libs.ws.WSClient
import play.api.libs.ws.ahc.AhcWSClient

object AppModule extends AbstractModule {

  override def configure() = {
    bind(classOf[Config]).toInstance(ConfigFactory.load())
    bind(classOf[ActorSystem]).toInstance(ActorSystem("hive-asset-service"))
    bind(classOf[AtlasKafkaConsumer]).asEagerSingleton()
  }

  @Provides
  @Singleton
  def provideMaterializer(implicit actorSystem: ActorSystem): ActorMaterializer = {
    ActorMaterializer()
  }

  @Provides
  @Singleton
  def provideWsClient(implicit actorSystem: ActorSystem,
                      materializer: ActorMaterializer): WSClient = {
    AhcWSClient()
  }

  @Provides
  @Singleton
  def providesRoutes(system: ActorSystem,
                     actorMaterailizer: ActorMaterializer,
                     agentClient: ProfilerAgentClient, config: Config): Route = {
    new DiscoverAssetRoute(system, actorMaterailizer, agentClient, config).route
  }

  @Provides
  @Singleton
  def provideAgentClient(config: Config, wsClient: WSClient): ProfilerAgentClient = {
    new ProfilerAgentClient(config.getString("profiler.agent.uri"), wsClient)
  }

}
