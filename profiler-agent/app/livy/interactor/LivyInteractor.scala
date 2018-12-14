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

package livy.interactor

import javax.inject.{Inject, Singleton}
import job.interactive.LivySessionNotRunningException
import livy.session.models.LivyRequests._
import livy.session.models.LivyResponses._
import livy.session.models.parsers.RequestParsers._
import livy.session.models.parsers.ResponseParsers._
import play.api.http.Status
import play.api.libs.json.{JsNull, Json}
import play.api.libs.ws.{WSAuthScheme, WSClient, WSRequest}
import play.api.{Configuration, Logger}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

@Singleton
class LivyInteractor @Inject()(configuration: Configuration, ws: WSClient) {

  private val livyurl = configuration.getString("livy.url").get
  private val secured = configuration.getBoolean("dpprofiler.secured").getOrElse(false)

  val SESSION_DEAD_MESSAGE: String = configuration.getString("livy.session.dead.message")
    .getOrElse("java.lang.IllegalStateException: Session is in state dead")
  val SESSION_STARTING_MESSAGE: String = configuration.getString("livy.session.start.message")
    .getOrElse("java.lang.IllegalStateException: Session is in state starting")


  def deleteSession(sessionId: Int): Future[Unit] = {
    val sessionUrl = s"$livyurl/sessions/$sessionId"
    withAuth(ws.url(sessionUrl))
      .withHeaders("Content-Type" -> "application/json", "X-Requested-By" -> "profiler")
      .delete() map (
      response => {
        response.status match {
          case Status.OK =>
            Logger.info(s"Deleted livy session $sessionId")
          case Status.NOT_FOUND =>
            Logger.warn(s"Trying to delete dead livy session $sessionId")
          case someOtherCode =>
            Logger.info(s"Livy session deletion Failed, status Code: $someOtherCode , response : ${response.body}")
        }
      }
      )
  }

  def createNewSession(createSession: CreateSession): Future[Int] = {
    val sessionUrl = s"$livyurl/sessions"
    val request = Json.toJson(createSession)
    withAuth(ws.url(sessionUrl))
      .withHeaders("Content-Type" -> "application/json", "X-Requested-By" -> "profiler")
      .post(request).flatMap {
      response =>
        response.status match {
          case Status.CREATED =>
            Logger.info(s"Created Interactive session :  ${response.json}")
            Future.successful((response.json \ "id").as[Int])
          case _ =>
            val errorMessage = s"Interactive session creation failed with code:  ${response.status}, ${response.body}"
            Logger.error(errorMessage)
            Future.failed(LivyInteractorException(errorMessage))
        }
    }
  }

  def getSessionState(sessionId: Int): Future[SessionState] = {
    val sessionUrl = s"$livyurl/sessions/$sessionId/state"
    withAuth(ws.url(sessionUrl))
      .withHeaders("Content-Type" -> "application/json", "X-Requested-By" -> "profiler")
      .get() flatMap (
      response => {
        response.status match {
          case Status.OK =>
            Future.successful(response.json.as[SessionState])
          case Status.NOT_FOUND =>
            Future.successful(SessionState(sessionId, StateOfSession.dead))
          case _ =>
            Future.failed(LivyInteractorException("Failed to get session state from livy"))
        }
      }
      )
  }

  def getSession(sessionId: Int): Future[Session] = {
    val sessionUrl = s"$livyurl/sessions/$sessionId"
    withAuth(ws.url(sessionUrl))
      .withHeaders("Content-Type" -> "application/json", "X-Requested-By" -> "profiler")
      .get() flatMap (
      response => {
        response.status match {
          case Status.OK =>
            Future.successful(response.json.as[Session])
          case _ =>
            Future.failed(LivyInteractorException("Failed to get session from livy"))
        }
      }
      )
  }

  def submitStatement(sessionId: Int, statement: SubmitStatement): Future[Int] = {
    val statementUrl = s"$livyurl/sessions/$sessionId/statements"
    val statementInLocalScope = statement.copy(code = encloseInLocalScope(statement.code))
    val request = Json.toJson(statementInLocalScope)
    withAuth(ws.url(statementUrl))
      .withHeaders("Content-Type" -> "application/json", "X-Requested-By" -> "profiler")
      .post(request).flatMap {
      response =>
        response.status match {
          case Status.CREATED =>
            Future.successful(response.json.as[Statement].id)
          case status if (status == Status.NOT_FOUND) || (status == Status.INTERNAL_SERVER_ERROR
            && response.body.contains(SESSION_DEAD_MESSAGE)) =>
            Future.failed(new LivySessionNotRunningException("Session not found or dead." +
              " Initializing session again"))
          case _ =>
            val errorMessage = s"Error while submitting statement:  ${response.status}, ${response.body}"
            Logger.error(errorMessage)
            Future.failed(LivyInteractorException(errorMessage))
        }
    }
  }

  def getStatement(sessionId: Int, statementId: Int): Future[Statement] = {
    val statementUrl = s"$livyurl/sessions/$sessionId/statements/$statementId"
    withAuth(ws.url(statementUrl))
      .withHeaders("Content-Type" -> "application/json", "X-Requested-By" -> "profiler")
      .get() flatMap (
      response => {
        response.status match {
          case Status.OK =>
            Future.successful(response.json.as[Statement])
          case status if (status == Status.NOT_FOUND) || (status == Status.INTERNAL_SERVER_ERROR
            && response.body.contains(SESSION_DEAD_MESSAGE)) =>
            Future.failed(new LivySessionNotRunningException("Session not found or dead." +
              " Initializing session again"))
          case _ =>
            Future.failed(LivyInteractorException("Failed to get Statement from livy"))
        }
      }
      )
  }

  def getStatements(sessionId: Int): Future[Statements] = {
    val statementUrl = s"$livyurl/sessions/$sessionId/statements"
    withAuth(ws.url(statementUrl))
      .withHeaders("Content-Type" -> "application/json", "X-Requested-By" -> "profiler")
      .get() flatMap (
      response => {
        response.status match {
          case Status.OK =>
            Future.successful(response.json.as[Statements])
          case status if (status == Status.NOT_FOUND) || (status == Status.INTERNAL_SERVER_ERROR
            && response.body.contains(SESSION_DEAD_MESSAGE)) =>
            Future.failed(new LivySessionNotRunningException("Session not found or dead." +
              " Initializing session again"))
          case _ =>
            Future.failed(LivyInteractorException("Failed to get Statement from livy"))
        }
      }
      )
  }

  def submitBatchJob(job: SubmitBatchJob): Future[Batch] = {
    val sessionUrl = s"$livyurl/batches"
    val request = Json.toJson(job)
    withAuth(ws.url(sessionUrl))
      .withHeaders("Content-Type" -> "application/json", "X-Requested-By" -> "profiler")
      .post(request).flatMap {
      response =>
        response.status match {
          case Status.CREATED =>
            Logger.info(s"Created Batch job :  ${response.json}")
            Future.successful(response.json.as[Batch])
          case _ =>
            val errorMessage = s"Batch job creation failed with code:  ${response.status}, ${response.body}"
            Logger.error(errorMessage)
            Future.failed(LivyInteractorException(errorMessage))
        }
    }
  }


  def getAllBatchJobs(request: GetBatches): Future[Batches] = {
    val sessionUrl = s"$livyurl/batches"
    withAuth(ws.url(sessionUrl))
      .withHeaders("Content-Type" -> "application/json", "X-Requested-By" -> "profiler")
      .withQueryString("from" -> request.from.toString, "size" -> request.size.toString)
      .get().flatMap {
      response =>
        response.status match {
          case Status.OK =>
            Future.successful(response.json.as[Batches])
          case _ =>
            val errorMessage = s"Getting all batch jobs failed with code:  ${response.status}, ${response.body}"
            Logger.error(errorMessage)
            Future.failed(LivyInteractorException(errorMessage))
        }
    }
  }

  def getBatchJob(batchId: Int): Future[Batch] = {
    val sessionUrl = s"$livyurl/batches/$batchId"
    withAuth(ws.url(sessionUrl))
      .withHeaders("Content-Type" -> "application/json", "X-Requested-By" -> "profiler")
      .get().flatMap {
      response =>
        response.status match {
          case Status.OK =>
            Future.successful(response.json.as[Batch])
          case Status.NOT_FOUND =>
            Future.successful(Batch(batchId, None, JsNull, Nil, StateOfBatchJob.not_found))
          case _ =>
            val errorMessage = s"Getting batch job with Id $batchId failed with code:  ${response.status}, ${response.body}"
            Logger.error(errorMessage)
            Future.failed(LivyInteractorException(errorMessage))
        }
    }
  }

  def getBatchJobLog(request: GetBatchLog): Future[BatchLog] = {
    val sessionUrl = s"$livyurl/batches/${request.id}/log"
    withAuth(ws.url(sessionUrl))
      .withHeaders("Content-Type" -> "application/json", "X-Requested-By" -> "profiler")
      .withQueryString("from" -> request.from.toString, "size" -> request.size.toString)
      .get().flatMap {
      response =>
        response.status match {
          case Status.OK =>
            Future.successful(response.json.as[BatchLog])
          case _ =>
            val errorMessage = s"Getting logs for batch job with Id ${request.id} failed with code:  ${response.status}, ${response.body}"
            Logger.error(errorMessage)
            Future.failed(LivyInteractorException(errorMessage))
        }
    }
  }


  private def encloseInLocalScope(interpretableCode: String): String = {
    s"""|
        |{
        | $interpretableCode
        | }
     """.stripMargin
  }


  private def withAuth(wSRequest: WSRequest): WSRequest = {
    if (secured) wSRequest.withAuth("", "", WSAuthScheme.SPNEGO)
    else wSRequest
  }
}
