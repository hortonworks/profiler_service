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

import com.hortonworks.dataplane.profilers.commons.domain.AssetType.AssetType
import domain.JobType.JobType
import domain._
import org.joda.time.DateTime
import play.api.db.slick.DatabaseConfigProvider
import play.api.libs.json.{JsObject, Json}
import slick.jdbc.JdbcProfile

import scala.concurrent.Future

@Singleton
class ProfilerRepo @Inject()(protected val dbConfigProvider: DatabaseConfigProvider) {

  import domain.JsonFormatters._

  val dbConfig = dbConfigProvider.get[JdbcProfile]
  val db = dbConfig.db

  import dbConfig.profile.api._

  val Profilers = TableQuery[ProfilersTable]

  def save(profiler: Profiler): Future[Profiler] = {
    db.run {
      (Profilers returning Profilers.map(_.id) into ((profiler, id) => profiler.copy(id = id))) += profiler.copy(created = Some(DateTime.now()))
    }
  }

  def update(profiler: Profiler): Future[Int] = {
    db.run {
      Profilers.filter(e => e.name === profiler.name && e.version === profiler.version).update(profiler)
    }
  }

  def findAll(): Future[Seq[Profiler]] = {
    db.run(Profilers.result)
  }

  def findOne(id: Long): Future[Option[Profiler]] = {
    db.run(Profilers.filter(_.id === id).result.headOption)
  }

  def findByNameAndVersion(name: String, version: String): Future[Option[Profiler]] = {
    db.run(Profilers.filter(e => e.name === name && e.version === version).result.headOption)
  }

  implicit val assetTypeColumnType = MappedColumnType.base[AssetType, String](
    j => Json.toJson(j).toString(),
    s => Json.parse(s).as[AssetType]
  )

  implicit val jobTypeColumnType = MappedColumnType.base[JobType, String](
    j => Json.toJson(j).toString(),
    s => Json.parse(s).as[JobType]
  )

  implicit val jsonColumnType = MappedColumnType.base[JsObject, String](
    json => json.toString(),
    s => Json.parse(s).as[JsObject]
  )


  implicit val localDateToDate = MappedColumnType.base[DateTime, Timestamp](
    l => new Timestamp(l.getMillis),
    d => new DateTime(d)
  )

  final class ProfilersTable(tag: Tag) extends Table[Profiler](tag, Some("profileragent"), "profilers") {

    def id = column[Option[Long]]("id", O.PrimaryKey, O.AutoInc)

    def name = column[String]("name")

    def version = column[String]("version")

    def jobType = column[JobType]("jobtype")

    def assetType = column[AssetType]("assettype")

    def profilerConf = column[JsObject]("profilerconf")

    def user = column[String]("user")

    def description = column[String]("description")

    def created = column[Option[DateTime]]("created")

    def * = (id, name, version, jobType, assetType, profilerConf, user, description, created) <> ((Profiler.apply _) tupled, Profiler.unapply)
  }

}