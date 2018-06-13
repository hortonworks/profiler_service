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
import job.runner.ProfilerAgentException
import org.joda.time.DateTime
import play.api.db.slick.DatabaseConfigProvider
import play.api.i18n.{I18nSupport, Messages, MessagesApi}
import play.api.libs.json.{JsObject, Json}
import slick.jdbc.JdbcProfile

import scala.concurrent.Future

@Singleton
class ProfilerInstanceRepo @Inject()(protected val dbConfigProvider: DatabaseConfigProvider,
                                     profilerRepo: ProfilerRepo, val messagesApi: MessagesApi) extends I18nSupport {

  import domain.JsonFormatters._
  import scala.concurrent.ExecutionContext.Implicits.global

  val dbConfig = dbConfigProvider.get[JdbcProfile]
  val db = dbConfig.db

  import dbConfig.profile.api._

  val ProfilerInstances = TableQuery[ProfilerInstancesTable]

  def save(profiler: ProfilerInstance): Future[ProfilerAndProfilerInstance] = {
    db.run {
      (ProfilerInstances returning ProfilerInstances.map(_.id) into ((profiler, id) => profiler.copy(id = id))) +=
        profiler.copy(created = Some(DateTime.now()))
    }.flatMap(p => findByName(profiler.name))
  }

  def update(profiler: ProfilerInstance) = {
    findByName(profiler.name).flatMap {
      prevProfiler =>
        val profilerToSave = profiler.copy(
          version = prevProfiler.profilerInstance.version + 1,
          created = Some(DateTime.now()), active = prevProfiler.profilerInstance.active)
        save(profilerToSave)
    }.flatMap(p => findByName(profiler.name))
  }


  def findAll(): Future[List[ProfilerAndProfilerInstance]] = {
    db.run {
      ProfilerInstances.join(profilerRepo.Profilers).on(_.profilerId === _.id)
        .map(e => (e._2, e._1))
        .to[List].result.map {
        res =>
          res.map {
            case (p, pi) => ProfilerAndProfilerInstance(p, pi)
          }
      }
    }
  }

  def findAllActive(): Future[List[ProfilerAndProfilerInstance]] = {
    db.run {
      ProfilerInstances.filter(_.active === true).join(profilerRepo.Profilers).on(_.profilerId === _.id)
        .map(e => (e._2, e._1))
        .to[List].result.map {
        res =>
          res.map {
            case (p, pi) => ProfilerAndProfilerInstance(p, pi)
          }
      }
    }
  }

  def findOne(id: Long): Future[Option[ProfilerAndProfilerInstance]] = {
    db.run {
      ProfilerInstances.filter(_.id === id).join(profilerRepo.Profilers).on(_.profilerId === _.id)
        .map(e => (e._2, e._1)).result.headOption
        .map(po => po.map(p => ProfilerAndProfilerInstance(p._1, p._2)))
    }
  }

  def updateState(name: String, active: Boolean): Future[ProfilerAndProfilerInstance] = {
    db.run {
      ProfilerInstances.filter(_.name === name).map(_.active).update(active)
    }.flatMap(p => findByName(name))
  }

  def findByNameOpt(name: String): Future[Option[ProfilerAndProfilerInstance]] = {
    db.run {
      ProfilerInstances.filter(_.name === name).sortBy(_.version.desc).take(1).join(profilerRepo.Profilers).on(_.profilerId === _.id)
        .map(e => (e._2, e._1)).result.headOption
        .map(po => po.map(p => ProfilerAndProfilerInstance(p._1, p._2)))
    }
  }

  def findByName(name: String): Future[ProfilerAndProfilerInstance] = {
    findByNameOpt(name)
      .map(_.getOrElse(throw new ProfilerAgentException(
        Messages("profiler.not.found.code"),
        Messages("profiler.not.found.message", name)
      )))
  }

  def findByNameAndVersion(name: String, version: Long): Future[Option[ProfilerInstance]] = {
    db.run(ProfilerInstances.filter(e => e.name === name && e.version === version)
      .result.headOption)
  }

  implicit val jsonColumnType = MappedColumnType.base[JsObject, String](
    json => json.toString(),
    s => Json.parse(s).as[JsObject]
  )


  implicit val localDateToDate = MappedColumnType.base[DateTime, Timestamp](
    l => new Timestamp(l.getMillis),
    d => new DateTime(d)
  )

  final class ProfilerInstancesTable(tag: Tag) extends Table[ProfilerInstance](tag, Some("profileragent"), "profilerinstances") {

    def id = column[Option[Long]]("id", O.PrimaryKey, O.AutoInc)

    def name = column[String]("name")

    def displayName = column[String]("displayname")

    def profilerId = column[Long]("profilerid")

    def version = column[Long]("version")

    def profilerConf = column[JsObject]("profilerconf")

    def jobConf = column[JsObject]("jobconf")

    def active = column[Boolean]("active")

    def owner = column[String]("owner")

    def queue = column[String]("queue")

    def description = column[String]("description")

    def created = column[Option[DateTime]]("created")

    def * = (id, name, displayName, profilerId, version, profilerConf, jobConf, active, owner, queue, description, created) <> ((ProfilerInstance.apply _) tupled, ProfilerInstance.unapply)
  }

}