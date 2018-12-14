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
import org.joda.time.DateTime
import play.api.libs.json.{JsValue, Json}
import profilers.dryrun.models.DryRunInstance
import profilers.dryrun.models.Responses.DryRunStatus
import profilers.dryrun.models.Responses.DryRunStatus.DryRunStatus

import scala.concurrent.Future

@Singleton
class DryRunRepo @Inject()(protected val dbConfigProvider: ProfilerDatabaseConfigProvider) {

  val dbConfig = dbConfigProvider.dbConfig
  val db = dbConfig.db

  import dbConfig.profile.api._

  val dryRunTable = TableQuery[DryRunTable]

  def save(dryRunInstance: DryRunInstance): Future[DryRunInstance] = {
    db.run {
      (dryRunTable returning dryRunTable.map(_.id) into ((profiler, id) => profiler.copy(id = id))) += dryRunInstance.copy(created = Some(DateTime.now()))
    }
  }


  def update(dryRunInstance: DryRunInstance): Future[Int] = {
    db.run {
      dryRunTable.filter(e => e.id === dryRunInstance.id).update(dryRunInstance)
    }
  }

  def findAll(): Future[Seq[DryRunInstance]] = {
    db.run(dryRunTable.result)
  }

  def findOne(id: Long): Future[Option[DryRunInstance]] = {
    db.run(dryRunTable.filter(_.id === id).result.headOption)
  }

  implicit val dryRunStatusFormatter = MappedColumnType.base[DryRunStatus, String](
    { c => c.toString },
    { s => DryRunStatus.withName(s) }
  )

  def findAllWithStatus(status: DryRunStatus): Future[Seq[DryRunInstance]] = {
    db.run(dryRunTable.filter(_.dryRunStatus === status).result)
  }


  def deleteOne(id: Long): Future[Int] = {
    db.run(dryRunTable.filter(_.id === id).delete)
  }


  implicit val jsonColumnType = MappedColumnType.base[JsValue, String](
    json => json.toString(),
    s => Json.parse(s)
  )


  implicit val localDateToDate = MappedColumnType.base[DateTime, Timestamp](
    l => new Timestamp(l.getMillis),
    d => new DateTime(d)
  )


  final class DryRunTable(tag: Tag) extends Table[DryRunInstance](tag, Some("profileragent"), "dryrun") {

    def id = column[Option[Long]]("id", O.PrimaryKey, O.AutoInc)

    def profiler = column[String]("profiler")

    def profilerInstance = column[String]("profilerinstance")

    def dryRunStatus = column[DryRunStatus]("dryrunstatus")

    def outputFile = column[String]("outputfile")

    def results = column[JsValue]("results")

    def errorMessage = column[JsValue]("errormessage")

    def created = column[Option[DateTime]]("created")

    def applicationid = column[Int]("applicationid")


    def * = (id, profiler, profilerInstance, dryRunStatus, outputFile, results, errorMessage, created, applicationid) <> (DryRunInstance.apply _ tupled, DryRunInstance.unapply)
  }

}