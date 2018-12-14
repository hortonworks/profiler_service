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

package livy.session.provider

import akka.actor.{ActorRef, Scheduler}
import livy.interactor.LivyInteractor
import livy.session.messages.SessionProviderInteractions.{HereIsYourNewSession, SessionCreationFailedAfterLotOfRetries}
import livy.session.messages.SessionRepositoryInteractions.{DeletedSessionId, NewSessionId}
import livy.session.models.LivyRequests.CreateSession
import livy.session.provider.SessionProviderHelper.{activeSessionStates, numberOfMillisInMinutes, startingUpSessionStates}
import play.api.Logger

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

class SessionCreator(interactor: LivyInteractor, scheduler: Scheduler,
                     sessionProvider: ActorRef, repository: ActorRef,
                     sessionConfig: CreateSession) {
  def createNewSessionAndMonitor(reTryCount: Int = 0): Unit = {
    if (reTryCount >= 20) {
      sessionProvider ! SessionCreationFailedAfterLotOfRetries(reTryCount)
    }
    else {
      Logger.debug(s" Trying to create a new session")
      val eventualSession = interactor.createNewSession(sessionConfig)
      eventualSession onFailure {
        case error => Logger.error("Session creation failed, Retrying after 5 seconds", error)
          akka.pattern.after(5.seconds, scheduler)(Future(createNewSessionAndMonitor(reTryCount + 1)))
      }
      eventualSession.map(sessionId => {
        val creationTimeInMinutes = System.currentTimeMillis() / numberOfMillisInMinutes
        repository ! NewSessionId(sessionId, creationTimeInMinutes)
        waitForSessionToBecomeActive(sessionId, creationTimeInMinutes, reTryCount)
      })
    }
  }


  private def waitForSessionToBecomeActive(sessionId: Int, creationTimeInMinutes: Long, reTryCount: Int): Unit = {
    Logger.debug(s"Waiting for session $sessionId to become active")
    val eventualState = interactor.getSessionState(sessionId)
    eventualState onFailure {
      case error =>
        Logger.error(s"Session $sessionId state check failed, Retrying after 1 second", error)
        akka.pattern.after(1.seconds, scheduler)(Future(waitForSessionToBecomeActive(sessionId,
          creationTimeInMinutes, reTryCount)))
    }

    eventualState.map(
      state =>
        state.state match {
          case x if activeSessionStates.contains(x) =>
            sessionProvider ! HereIsYourNewSession(sessionId, creationTimeInMinutes)
          case y if startingUpSessionStates.contains(y) =>
            akka.pattern.after(3.seconds, scheduler)(Future(waitForSessionToBecomeActive(sessionId,
              creationTimeInMinutes, reTryCount)))
          case z =>
            repository ! DeletedSessionId(sessionId)
            interactor.deleteSession(sessionId)
            Logger.error(s"Lost newly created session $sessionId; state: $z , retrying after 5 seconds")
            akka.pattern.after(5.seconds, scheduler)(Future(createNewSessionAndMonitor(reTryCount + 1)))
        }
    )
  }

}
