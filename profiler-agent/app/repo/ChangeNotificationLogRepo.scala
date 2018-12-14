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
import java.time.Instant
import javax.inject.{Inject, Singleton}

import domain.ChangeNotificationLog
import org.joda.time.DateTime
import play.api.libs.json.Json

import scala.concurrent.Future

@Singleton
class ChangeNotificationLogRepo @Inject()(protected val dbConfigProvider: ProfilerDatabaseConfigProvider) {

  import domain.JsonFormatters._

  val dbConfig = dbConfigProvider.dbConfig
  val db = dbConfig.db

  import dbConfig.profile.api._

  val ChangeNotificationLogs = TableQuery[ChangeNotificationLogTable]

  def add(changeLogs: Seq[ChangeNotificationLog]) ={
    db.run(ChangeNotificationLogs ++= changeLogs)
  }

  def findLastSavedEventId: Future[Option[Long]] ={
    db.run(ChangeNotificationLogs.map(_.eventId).max.result)
  }

  def getAllInRange(startTime: Option[Long], endTime: Option[Long]): Future[Seq[ChangeNotificationLog]] = {

    val logsQuery = ChangeNotificationLogs

    val logsFilterOnStart = startTime.map { start =>
      logsQuery.filter(_.recordedTime > start)
    }.getOrElse(logsQuery)

    val logsFilterOnEnd = endTime.map { end =>
      logsFilterOnStart.filter(_.recordedTime <= end)
    }.getOrElse(logsFilterOnStart)

    db.run(logsFilterOnEnd.result)
  }

  def deleteOlder(keepRange: Long): Future[Int] = {
    val deleteBefore = Instant.now().toEpochMilli - keepRange
    db.run(ChangeNotificationLogs.filter(r => r.recordedTime <= deleteBefore).delete)
  }

  implicit val mapSeqColumnType = MappedColumnType.base[Seq[Map[String, String]], String](
    e => Json.toJson(e).toString,
    s => Json.parse(s).as[Seq[Map[String, String]]]
  )

  final class ChangeNotificationLogTable(tag: Tag) extends Table[ChangeNotificationLog](tag, Some("profileragent"), "change_notification_log") {

    def id = column[Option[Long]]("id", O.PrimaryKey, O.AutoInc)

    def eventId = column[Long]("eventid")

    def tableId = column[String]("tableid")

    def eventTime = column[Int]("eventtime")

    def eventType = column[String]("eventtype")

    def partitions = column[Seq[Map[String, String]]]("partitions")

    def message = column[Option[String]]("message")

    def recordedTime = column[Long]("recordedtime")

    def * = (id, eventId, tableId, eventTime, eventType, partitions, message, recordedTime) <> ((ChangeNotificationLog.apply _) tupled, ChangeNotificationLog.unapply)
  }

}