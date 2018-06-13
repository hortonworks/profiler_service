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
import domain.JobType.JobType
import domain._
import org.joda.time.DateTime
import play.api.db.slick.DatabaseConfigProvider
import play.api.libs.json.{JsObject, Json}
import slick.jdbc.JdbcProfile
import slick.lifted.ColumnOrdered

import scala.concurrent.Future

@Singleton
class ProfilerJobRepo @Inject()(protected val dbConfigProvider: DatabaseConfigProvider,
                                profilerInstanceRepo: ProfilerInstanceRepo,
                                assetJobHistoryRepo: AssetJobHistoryRepo) {
  val dbConfig = dbConfigProvider.get[JdbcProfile]
  val db = dbConfig.db

  import dbConfig.profile.api._
  import scala.concurrent.ExecutionContext.Implicits.global

  val ProfilerJobs = TableQuery[ProfilerJobsTable]

  implicit val localDateToDate = MappedColumnType.base[DateTime, Timestamp](
    l => new Timestamp(l.getMillis),
    d => new DateTime(d)
  )

  def insert(profilerJob: ProfilerJob): Future[ProfilerJob] = {
    val profilerJobCopy = profilerJob.copy(start= DateTime.now, lastUpdated = DateTime.now)
    db.run {
      (ProfilerJobs returning ProfilerJobs.map(_.id) into ((profiler, id) => profiler.copy(id = id))) += profilerJobCopy
    }
  }

  def update(profilerJob: ProfilerJob): Future[Int] = {
    db.run {
      ProfilerJobs.filter(_.id === profilerJob.id).update(profilerJob)
    }
  }

  def getById(id: Long): Future[Option[ProfilerJob]] = {
    db.run(ProfilerJobs.filter(_.id === id).result.headOption)
  }

  def getJobs(skip: Long, limit: Long, submitter: Option[String], scheduled: Option[Boolean]): Future[List[ProfilerJob]] = {
    val query = ProfilerJobs
    val queryWithSubmitter = submitter
      .map(s => query.filter(_.submitter === s))
      .getOrElse(query)

    val queryWithSubmitterAndScheduled = scheduled
      .map(s => queryWithSubmitter.filter(_.queue === ""))
      .getOrElse(queryWithSubmitter)

    db.run(queryWithSubmitterAndScheduled.drop(skip).take(limit).to[List].result)
  }

  def getOperatedAssetsCount(startDate: DateTime, endDate: DateTime) = {

    val assetCountQuery = for{
      ((pid, pname, pdisplayname, pactive, pversion), res) <- profilerInstanceRepo.ProfilerInstances
        .joinLeft(ProfilerJobs.filter(t => (t.start >= startDate && t.start <= endDate)))
        .on(_.id === _.profilerInstanceId)
        .joinLeft(assetJobHistoryRepo.AssetsJobsHistory)
        .on(_._2.flatMap(_.id) === _.jobId)
        .groupBy(joinedTable => (joinedTable._1._1.id, joinedTable._1._1.name, joinedTable._1._1.displayName, joinedTable._1._1.active, joinedTable._1._1.version))
    } yield (pid, pname, pdisplayname, pactive, pversion, res.map(_._2.map(_.assetId)).countDistinct)    // suggested distinct.length throws exception in groupBy because of a bug. See here https://github.com/slick/slick/issues/1760

    db.run(assetCountQuery.result).map { results =>
      results.map{ res =>

        val profilerJobInfo = ProfilerInfoRefined(id = res._1, name = res._2, displayName = res._3, active = res._4, version = res._5)

        ProfilerAssetsCount(profilerJobInfo, assetsCount = res._6)
      }
    }

  }

  def getJobCounts(startDate: DateTime, endDate: DateTime) = {

    val jobCountQuery = for {
      ((profilerId, profilerName, pDisplayName, pActive, pVersion, jobStatus), joinedResults) <- ProfilerJobs.filter(t => (t.start >= startDate && t.start <= endDate))
        .joinRight(profilerInstanceRepo.ProfilerInstances)
        .on(_.profilerInstanceId === _.id)
        .groupBy( joinedTable => (joinedTable._2.id, joinedTable._2.name, joinedTable._2.displayName,joinedTable._2.active, joinedTable._2.version, joinedTable._1.map(_.status)))
    } yield (profilerId, profilerName,pDisplayName, pActive, pVersion, jobStatus, joinedResults.map(_._1).length)

    db.run(jobCountQuery.result).map { results =>
      results.groupBy(_._1).map {
        case(id, values) =>
          val jobsCount = values.groupBy(_._6).map{
            case(k,v) =>
              if(k.isDefined) (k.get.toString, v.head._7)
              else (JobStatus.UNKNOWN.toString, 0)  //None will come only when there is no entry for that profiler in ProfilerJobs table. So it should be safe to put UNKNOWN status count as 0, as count is going to be zero for all statuses (including UNKNOWN) anyway
          }.toMap

          val resVal = values.head
          val profilerJobInfo = ProfilerInfoRefined(id = resVal._1, name = resVal._2, displayName = resVal._3, active = resVal._4, version = resVal._5)
          ProfilerJobsCount(profilerJobInfo, JobStatus.defaultMap ++ jobsCount)
      }.toSeq
    }

  }

  def getJobsOnFilters(startTimeMs: Option[Long], endTimeMs: Option[Long], status: Seq[String], profilerIds: Seq[Long], paginatedQuery: Option[PaginatedQuery]) = {

    val jobsQuery = ProfilerJobs

    val jobsFilterOnStartQuery = startTimeMs.map { start =>

      jobsQuery.filter(_.start >= new DateTime(start))

    }.getOrElse(jobsQuery)

    val jobsFilterOnEndQuery = endTimeMs.map {end =>

      jobsFilterOnStartQuery.filter((_.start <= new DateTime(end)))

    }.getOrElse(jobsFilterOnStartQuery)

    val jobsFilteredOnStatusQuery = if (status.nonEmpty) {

      val jobStatuses: Seq[JobStatus.JobStatus] = status.map { st =>
        JobStatus.withName(st)
      }

      jobsFilterOnEndQuery.filter(_.status.inSet(jobStatuses))

    } else {

      jobsFilterOnEndQuery

    }

    val jobsFilteredOnStatusAndProfilerQuery = if (profilerIds.nonEmpty) {

      jobsFilteredOnStatusQuery.filter(_.profilerInstanceId.inSet(profilerIds))

    } else {

      jobsFilteredOnStatusQuery

    }


    val paginatedJobsQuery = sortByDataset(paginatedQuery, jobsFilteredOnStatusAndProfilerQuery)

    db.run(paginatedJobsQuery.result).map { results =>

      results.map { res =>

        FilteredProfilerJob(res.id, res.profilerInstanceId, res.profilerInstanceName, res.status, res.queue, (res.details \ "appInfo" \ "sparkUiUrl").asOpt[String], (res.details \ "appInfo" \ "driverLogUrl").asOpt[String], res.start, res.end)

      }

    }

  }

  def sortByDataset(paginationQuery: Option[PaginatedQuery],
                    query: Query[ProfilerJobsTable, ProfilerJobsTable#TableElementType, Seq]) = {

    paginationQuery.map {
      pq =>

        val q = pq.sortQuery.map {
          sq =>

            query.sortBy {
              oq =>

                sq.sortCol match {
                  case "id" => oq.id
                  case "status" => oq.status
                  case "start" => oq.start
                  case "end" => oq.end
                  case "queue" => oq.queue
                  case "profilers" => oq.profilerInstanceName

                }

            }(ColumnOrdered(_, sq.ordering))

        }.getOrElse(query)

        q.drop(pq.offset).take(pq.size)

    }
      .getOrElse(query)
  }

  implicit val jobTypeColumnType = MappedColumnType.base[JobType, String](
    e => e.toString,
    s => JobType.withName(s)
  )

  implicit val jobStatusColumnType = MappedColumnType.base[JobStatus, String](
    e => e.toString,
    s => JobStatus.withName(s)
  )

  implicit val jsObjColumnType = MappedColumnType.base[JsObject, String](
    json =>
      json.toString(),
    s =>
      Json.parse(s).as[JsObject]
  )

  final class ProfilerJobsTable(tag: Tag) extends Table[ProfilerJob](tag, Some("profileragent"), "jobs") {

    def id = column[Option[Long]]("id", O.PrimaryKey, O.AutoInc)

    def profilerInstanceId = column[Long]("profilerinstanceid")

    def profilerInstanceName = column[String]("profilerinstancename")

    def status = column[JobStatus]("status")

    def jobType = column[JobType]("jobtype")

    def conf = column[JsObject]("conf")

    def details = column[JsObject]("details")

    def description = column[String]("description")

    def jobUser = column[String]("jobuser")

    def queue = column[String]("queue")

    def submitter = column[String]("submitter")

    def start = column[DateTime]("start")

    def lastUpdated = column[DateTime]("lastupdated")

    def end = column[Option[DateTime]]("end")

    def * = (id, profilerInstanceId, profilerInstanceName, status, jobType, conf, details, description, jobUser, queue, submitter, start, lastUpdated, end) <> ((ProfilerJob.apply _) tupled, ProfilerJob.unapply)
  }


}
