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
import javax.inject.Inject

import com.google.inject.Singleton
import domain.JobStatus.JobStatus
import domain._
import org.joda.time.DateTime
import play.api.db.slick.DatabaseConfigProvider
import slick.jdbc.JdbcProfile

import scala.concurrent.Future

@Singleton
class AssetJobHistoryRepo @Inject()(protected val dbConfigProvider: DatabaseConfigProvider,
                                    profilerInstanceRepo: ProfilerInstanceRepo) {

  val dbConfig = dbConfigProvider.get[JdbcProfile]
  val db = dbConfig.db

  import dbConfig.profile.api._
  import scala.concurrent.ExecutionContext.Implicits.global

  val AssetsJobsHistory = TableQuery[AssetJobsHistoryTable]

  implicit val localDateToDate = MappedColumnType.base[DateTime, Timestamp](
    l => new Timestamp(l.getMillis),
    d => new DateTime(d)
  )

  implicit val jobStatusColumnType = MappedColumnType.base[JobStatus, String](
    e => e.toString,
    s => JobStatus.withName(s)
  )

  def getAllRunningOrStarted(): Future[List[AssetJobHistory]] = {
    db.run(
      AssetsJobsHistory.filter(h => h.status === JobStatus.STARTED || h.status === JobStatus.RUNNING).to[List].result
    )
  }

  def getAllRunningInSeq(ids: Seq[String], profilerInstanceName: String): Future[List[AssetJobHistory]] = {
    db.run(
      AssetsJobsHistory.filter(h => h.assetId.inSet(ids) && h.profilerInstanceName === profilerInstanceName &&
        (h.status === JobStatus.STARTED || h.status === JobStatus.RUNNING)).to[List].result
    )
  }

  def getLastJob(assetId: String, profilerInstanceName: String): Future[Option[AssetJobHistory]] = {
    db.run(
      AssetsJobsHistory
        .filter(h => h.assetId === assetId && h.profilerInstanceName === profilerInstanceName)
        .sortBy(_.lastUpdated.desc.nullsLast)
        .take(1).result.headOption
    )
  }

  def saveAll(assets: Seq[Asset], job: ProfilerJob) = {
    val assetsHistory = AssetJobHistory.getAssetsHistory(assets, job)
    val ids = assetsHistory.map(_.assetId)

    val assetsHistoryCopy = assetsHistory.map { assetHistory =>
      assetHistory.copy(lastUpdated = DateTime.now())
    }
    val insertQuery = AssetsJobsHistory ++= assetsHistoryCopy
    db.run(insertQuery)
  }

  def updateJobStatus(jobId: Long, jobStatus: JobStatus): Future[Int] = {
    val q = for {asset <- AssetsJobsHistory if asset.jobId === jobId} yield asset.status
    db.run(
      q.update(jobStatus)
    )
  }

  def getAll = {
    db.run(AssetsJobsHistory.result)
  }

  def getOperatedAssetsCountPerDay(profilerName: String, startTime: Long, endTime:Long): Future[Seq[OperatedAssetCountsPerDay]] = {

    val startDateTime = new DateTime(startTime)
    val endDateTime = new DateTime(endTime)

    val dateFun = SimpleExpression.unary[DateTime, String] { (date, qb) =>
      qb.sqlBuilder += "CAST( "
      qb.expr(date)
      qb.sqlBuilder += " AS Date)"
    }

    val query = AssetsJobsHistory
      .filter(t => (t.profilerInstanceName === profilerName) && t.lastUpdated > startDateTime && t.lastUpdated <= endDateTime)
      .joinLeft(AssetsJobsHistory
        .filter(t => (t.profilerInstanceName === profilerName) && t.lastUpdated > startDateTime && t.lastUpdated <= endDateTime))
      .on((a, b) => a.assetId === b.assetId && dateFun(a.lastUpdated) === dateFun(b.lastUpdated) &&
        (a.lastUpdated < b.lastUpdated || a.lastUpdated === b.lastUpdated && a.id < b.id)) // join on a.lastUpdated < b.lastUpdated will cause the row with highest lastupdate in table 'a' to have empty RHS (i.e. no entry for table 'b' in that row)
      .filter(_._2.isEmpty)                                                                // filter rows where table 'b' is empty. That row will have highest lastupdate on that day.
      .groupBy(t => (dateFun(t._1.lastUpdated), t._1.status))
      .map(t => (t._1, t._2.length))


    db.run(query.result).map {results =>

      results.groupBy(_._1._1).map {

        case (date, values) =>

          val assetsCount = values.groupBy(_._1._2).map {

            case (k, v) =>
              (k.toString, v.head._2)

          }.toMap

          OperatedAssetCountsPerDay(date, JobStatus.defaultMap ++ assetsCount)

      }.toSeq.sortBy(entry => new DateTime(entry.day).getMillis)
    }
  }

  def getProfilersLastRunInfoOnAsset(assetId: String) = {

    val query = profilerInstanceRepo.ProfilerInstances
      .joinLeft(AssetsJobsHistory.filter(_.assetId === assetId)
        .joinLeft(AssetsJobsHistory.filter(t => t.assetId === assetId))
        .on((a,b) => a.profilerInstanceName === b.profilerInstanceName && (a.lastUpdated < b.lastUpdated || (a.lastUpdated === b.lastUpdated && a.id < b.id)))
        .filter(_._2.isEmpty))
      .on(_.name === _._1.profilerInstanceName)

    db.run(query.result).map {results =>

      results.map {res =>

        ProfilerRunInfo(res._1, res._2.map(_._1))

      }
    }

  }



  final class AssetJobsHistoryTable(tag: Tag) extends Table[AssetJobHistory](tag, Some("profileragent"), "assetjobshistory") {

    def id = column[Option[Long]]("id", O.PrimaryKey, O.AutoInc)

    def profilerInstanceName = column[String]("profilerinstancename")

    def assetId = column[String]("assetid")

    def jobId = column[Long]("jobid")

    def status = column[JobStatus]("status")

    def lastUpdated = column[DateTime]("lastupdated")

    def * = (id, profilerInstanceName, assetId, jobId, status, lastUpdated) <> ((AssetJobHistory.apply _) tupled, AssetJobHistory.unapply)
  }

}
