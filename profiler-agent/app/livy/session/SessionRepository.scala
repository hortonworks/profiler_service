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


package livy.session

import java.io.{File, PrintWriter}

import akka.actor.Actor
import livy.session.messages.SessionRepositoryInteractions._
import play.api.libs.json.Json.JsValueWrapper
import play.api.libs.json._
import play.api.{Configuration, Logger}

import scala.collection.mutable
import scala.io.Source

class SessionRepository(configuration: Configuration, sessionGroup: String) extends Actor {

  private implicit val mapFormat: Format[Map[Int, Long]] = new Format[Map[Int, Long]] {
    def reads(jv: JsValue): JsResult[Map[Int, Long]] =
      JsSuccess(jv.as[Map[String, Long]].map { case (k, v) =>
        Integer.parseInt(k) -> v.asInstanceOf[Long]
      })

    def writes(map: Map[Int, Long]): JsValue =
      Json.obj(map.map { case (s, o) =>
        val ret: (String, JsValueWrapper) = s.toString -> JsNumber(o)
        ret
      }.toSeq: _*)
  }

  private val livySessionFilePath = s"${configuration.getString("profiler.data.dir").getOrElse("/tmp")}/livy-sessions-$sessionGroup"

  private val sessionsInUse: mutable.LinkedHashMap[Int, Long] = new mutable.LinkedHashMap[Int, Long]()

  private val freeSessions: mutable.LinkedHashMap[Int, Long] = new mutable.LinkedHashMap[Int, Long]()

  loadLostSessions()


  override def receive: Receive = {
    case message =>
      Logger.debug(s"actor: ${self.path.name}, Message: $message, state :$myState")
      message match {
        case RetrieveLostSessionId =>
          provideLostSession()
        case NewSessionId(id, time) =>
          sessionsInUse += (id -> time)
          updateChangesInStorage()
        case DeletedSessionId(id) =>
          sessionsInUse.remove(id)
          freeSessions.remove(id)
          updateChangesInStorage()
        case unknowMessage =>
          Logger.error(s"Unknown message $unknowMessage ast actor SessionRepository")
      }
  }

  private def updateChangesInStorage(): Unit = {
    val pw = new PrintWriter(new File(livySessionFilePath))
    val allSessions = sessionsInUse ++ freeSessions
    val sessionsAsString = Json.stringify(Json.toJson(allSessions.toMap[Int, Long]))
    pw.write(sessionsAsString)
    pw.close()
  }


  private def provideLostSession() = {
    sender() ! {
      if (freeSessions.isEmpty)
        NothingToConsume
      else {
        val lostSessionId = freeSessions.head
        sessionsInUse += lostSessionId
        freeSessions.remove(lostSessionId._1)
        LostSessionId(lostSessionId._1, lostSessionId._2)
      }
    }
  }

  private def loadLostSessions(): Unit = {
    val livySessionFile = new File(livySessionFilePath)
    if (livySessionFile.exists()) {
      val sessionIds = Source.fromFile(livySessionFile).mkString
      val sessions = Json.parse(sessionIds).as[Map[Int, Long]]
      freeSessions.++=(sessions)
    }
  }

  private def myState: String = s"(sessionsInUse:$sessionsInUse,freeSessions:$freeSessions)"
}
