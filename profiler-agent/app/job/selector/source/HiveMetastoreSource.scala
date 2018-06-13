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

package job.selector.source

import java.util.concurrent.atomic.AtomicBoolean

import com.hortonworks.dataplane.profilers.commons.domain.AssetType
import domain.Asset
import job.selector.PullAssetSource
import job.selector.source.hive.HiveMetastoreClientFactory
import play.api.Configuration
import play.api.libs.json.{JsObject, Json}

import scala.collection.mutable
import scala.concurrent.Future

class HiveMetastoreSource(configuration: Configuration) extends PullAssetSource {

  private val hiveClient = HiveMetastoreClientFactory.get(configuration)
  private val done = new AtomicBoolean(false)

  import collection.JavaConverters._

  val databaseQueue = mutable.Queue[String](hiveClient.getAllDatabases.asScala: _*)

  override def fetchNext() = {
    if (databaseQueue.nonEmpty) Future.successful(getHiveAssets())
    else {
      done.getAndSet(true)
      Future.successful(Seq())
    }
  }

  private def getAsset(database: String, table: String) = {
    val id = s"${database}.${table}"
    val data = Json.obj(
      "id" -> id,
      "db" -> database,
      "table" -> table
    )
    Asset(id, AssetType.Hive, data)
  }

  private def getHiveAssets(): Seq[Asset] = {
    val database = databaseQueue.dequeue()
    val tables = hiveClient.getAllTables(database).asScala
    tables.map(t => getAsset(database, t))
  }

  override def isDone() = {
    Future.successful(done.get())
  }
}

object HiveMetastoreSource {
  def apply(sourceConfig: JsObject, configuration: Configuration) = new HiveMetastoreSource(configuration)
}
