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

package com.hortonworks.dataplane.profilers.asset.hive

import java.util
import java.util.UUID

import akka.actor.Actor
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, ThrottleMode}
import com.hortonworks.dataplane.profilers.commons.client.ProfilerAgentClient
import com.hortonworks.dataplane.profilers.commons.domain.{AssetType, HiveAssetObj}
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success}

class HiveAssetDiscoveryActor(implicit val materializer: ActorMaterializer,
                              config: Config, agentClient: ProfilerAgentClient) extends Actor with LazyLogging {

  implicit val system = context.system
  private val client = HiveMetastoreClientFactory.get(config)

  import scala.concurrent.ExecutionContext.Implicits.global

  private val databaseToProcess = ListBuffer[String]()
  private val processedDatabase = ListBuffer[String]()

  private val jobMap = collection.mutable.Map[String, DiscoveryStatus]()

  private val dbThrottle = config.getInt("throttled.hive.db.per.sec")
  private val throttledMessages = config.getInt("throttled.asset.per.sec")
  private val bulkSize = config.getInt("request.bulk.size")

  override def receive: Receive = waitForCrawl

  def waitForCrawl: Receive = {
    case DiscoverAll =>
      import collection.JavaConverters._
      databaseToProcess.clear()
      processedDatabase.clear()
      databaseToProcess ++= client.getAllDatabases.asScala

      import concurrent.duration._
      val jobId = UUID.randomUUID().toString()

      Source(databaseToProcess.toList)
        .throttle(dbThrottle, 1 second, 1, ThrottleMode.Shaping)
        .mapConcat(db => {
          logger.info(s"Getting tables for database: ${db}")
          val assets = client.getAllTables(db).asScala.toList.map(table => HiveAssetObj(db, table))
          processedDatabase += db
          assets
        })
        .throttle(throttledMessages, 1 second, bulkSize, ThrottleMode.Shaping)
        .grouped(bulkSize)
        //        .map(table => {
        //          println(table)
        //          table
        //        })
        //TODO Stop when we encounter status other than 200 when posting asset
        .mapAsync(1)(agentClient.postAssets(_, AssetType.Hive))
        .runWith(Sink.ignore)
        .onComplete {
          case Success(done) =>
            logger.info("Hive Asset Discovery finished successfully")
            jobMap.put(jobId, DiscoveryStatus(Right(100)))
            self ! DiscoveryFinished
          case Failure(e) =>
            logger.error(s"Hive Asset Discovery Failed. ${e.getMessage}", e)
            jobMap.put(jobId, DiscoveryStatus(Left(e)))
            self ! DiscoveryFinished
        }

      sender ! DiscoveryStarted(jobId)
      context.become(crawling(jobId))
    case GetDiscoveryStatus(id) =>
      sender ! jobMap.getOrElse(id, NoSuchDiscoveryId)
      logger.info("Not Crawling")
  }

  def crawling(id: String): Receive = {
    case GetDiscoveryStatus =>
      val percentage = processedDatabase.length * 100 / databaseToProcess.length
      sender ! DiscoveryStatus(Right(percentage))
      logger.info(s"Percentage done : ${percentage}")
    case DiscoverAll =>
      sender ! DiscoveryAlreadyInProgress(id)
      logger.info("Job already in process")
    case DiscoveryFinished =>
      context.become(waitForCrawl)
  }
}
