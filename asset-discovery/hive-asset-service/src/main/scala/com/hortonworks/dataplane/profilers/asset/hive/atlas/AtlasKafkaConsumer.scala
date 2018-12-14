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

package com.hortonworks.dataplane.profilers.asset.hive.atlas

import javax.inject.Inject

import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.Sink
import akka.stream.{ActorAttributes, ActorMaterializer, Supervision, ThrottleMode}
import com.hortonworks.dataplane.profilers.commons.client.ProfilerAgentClient
import com.hortonworks.dataplane.profilers.commons.domain.{AssetType, HiveAssetObj}
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.atlas.notification.NotificationInterface
import org.apache.atlas.notification.entity.EntityNotificationImpl
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

import scala.util.{Failure, Success}

class AtlasKafkaConsumer @Inject()(system: ActorSystem, implicit val materializer: ActorMaterializer,
                                   config: Config, agentClient: ProfilerAgentClient) extends LazyLogging {

  import scala.concurrent.ExecutionContext.Implicits.global

  private val entity = NotificationInterface.NotificationType.ENTITIES

  private val throttledMessages = config.getInt("throttled.asset.per.sec")
  private val bulkSize = config.getInt("request.bulk.size")

  import concurrent.duration._

  private val group = config.getString("atlas.kafka.entities.group.id")
  private val bootstrapServers = config.getString("atlas.kafka.bootstrap.servers")

  logger.info(s"Bootstrap servers : ${bootstrapServers}")
  logger.info(s"kafa consumer group : ${group}")

  private val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
    .withBootstrapServers(bootstrapServers)
    .withGroupId(group)
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")

  initStream()

  private def initStream(): Unit = {
    logger.info("Creating Atlas Kafka consumer for Hive")
    Consumer.plainSource(consumerSettings, Subscriptions.topics("ATLAS_ENTITIES"))
      .map(m => entity.getDeserializer().deserialize(m.value()).asInstanceOf[EntityNotificationImpl])
      .filter(m => m.getEntity.getId.typeName == "hive_table")
      .map(e => getHiveAsset(e.getEntity.get("qualifiedName").asInstanceOf[String]))
      .throttle(throttledMessages, 1 second, bulkSize, ThrottleMode.Shaping)
      .groupedWithin(bulkSize, 10 seconds)
      .mapAsync(1)(e => agentClient.postAssets(e.toSet.toSeq, AssetType.Hive).map(r => (r, e.toSet)))
      .withAttributes(ActorAttributes.supervisionStrategy(resumeAllDecider))
      .runWith(Sink.foreach({
        case (r, e) =>
          logger.info(s"Submitted hive asset. Size ${e.size} . Response : ${r}")
          logger.info(e.toString())
      }))
      .onComplete({
        //Should not reach here
        case Success(done) =>
          logger.info("Atlas kafka consumer finished successfully")
          initStream()
        case Failure(e) =>
          logger.error("Atlas kafka consumer failed with error", e)
          initStream()
      })
  }

  private def resumeAllDecider: Supervision.Decider = {
    case e: Throwable =>
      logger.error(e.getMessage, e)
      Supervision.Resume
    case a => {
      logger.warn(s"Got Error ${a.toString}")
      Supervision.Resume
    }
  }

  private def getHiveAsset(qualifiedName: String) = {
    val split = qualifiedName.split("\\.")
    HiveAssetObj(split(0), split(1).split("@")(0))
  }

}
