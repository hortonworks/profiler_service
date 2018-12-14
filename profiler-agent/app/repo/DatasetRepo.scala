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

package repo

import java.sql.Timestamp
import javax.inject.{Inject, Singleton}

import domain._
import org.joda.time.DateTime
import play.api.db.slick.DatabaseConfigProvider
import slick.jdbc.JdbcProfile

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

@Singleton
class DatasetRepo @Inject()(protected val dbConfigProvider: ProfilerDatabaseConfigProvider,
                            assetJobHistoryRepo: AssetJobHistoryRepo) {

  val dbConfig = dbConfigProvider.dbConfig
  val db = dbConfig.db

  import dbConfig.profile.api._

  val DatasetAssets = TableQuery[DatasetAssetIdTable]

  implicit val localDateToDate = MappedColumnType.base[DateTime, Timestamp](
    l => new Timestamp(l.getMillis),
    d => new DateTime(d)
  )

  def save(dsassets: Seq[DatasetAssetId]) = {
    db.run(DatasetAssets ++= dsassets)
  }

  def saveOrUpdate(dsAndAssetIds: DatasetAndAssetIds) = {

    val dsName = dsAndAssetIds.datasetName
    val dsAssetSeq = dsAndAssetIds.assetIds.map { dsAsset =>
      DatasetAssetId(None, dsName, dsAsset, Some(10000))
    }

    val query = (for {
      deleted <- DatasetAssets.filter(_.datasetName === dsName).delete
      inserted <- DatasetAssets ++= dsAssetSeq
    } yield(deleted,inserted)).transactionally

    db.run(query)

  }

  def deleteByDatasetName(dsName: String): Future[Int] ={
    db.run(DatasetAssets.filter(_.datasetName === dsName).delete)
  }

  def getByDsName(dsName: String): Future[Seq[DatasetAssetId]] = {
    db.run(DatasetAssets.filter(_.datasetName === dsName).result)
  }

  def assetExists(assetId: String): Future[Boolean] = {
    val query = DatasetAssets.filter(_.assetId === assetId).exists.result
    db.run(query)
  }

  def getAll = {
    db.run(DatasetAssets.result)
  }

  def profilerDatasetAssetCount(profilerinstancename: String, datasetName: String, startTime: Long, endTime: Long) = {

    val startDateTime = new DateTime(startTime)
    val endDateTime = new DateTime(endTime)

    val datasetAssetCountQuery = for {
      (dsName, res) <- DatasetAssets.filter(_.datasetName === datasetName)
        .join(assetJobHistoryRepo.AssetsJobsHistory
          .filter(t => t.profilerInstanceName === profilerinstancename).filter(t => t.lastUpdated > startDateTime && t.lastUpdated <= endDateTime))
        .on(_.assetId === _.assetId)
        .groupBy(_._1.datasetName)
    } yield (dsName, res.map(_._1.assetId).countDistinct)  //We should remove this but could not find other alternative for now. See here https://github.com/slick/slick/issues/1760

    db.run(datasetAssetCountQuery.result.headOption)
  }

  final class DatasetAssetIdTable(tag: Tag) extends Table[DatasetAssetId](tag, Some("profileragent"), "dataset_asset") {

    def id = column[Option[Long]]("id", O.PrimaryKey, O.AutoInc)

    def datasetName = column[String]("datasetname")

    def assetId = column[String]("assetid")

    def priority = column[Option[Long]]("priority")

    def * = (id, datasetName, assetId, priority) <> ((DatasetAssetId.apply _) tupled, DatasetAssetId.unapply)
  }

}