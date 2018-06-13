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

import akka.actor._
import livy.helper.GenericSupervisor
import livy.interactor.LivyInteractor
import livy.session.messages.SessionProviderInteractions._
import livy.session.messages.SessionRepositoryInteractions._
import livy.session.models.LivyRequests.CreateSession
import livy.session.provider.SessionProviderHelper.{SessionProviderStatus, _}
import play.api.{Configuration, Logger}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

class SessionProvider(interactor: LivyInteractor, repository: ActorRef,
                      configuration: Configuration, sessionConfig: CreateSession) extends Actor {

  private val expiredSessionTracker: ExpiredSessionTracker = new ExpiredSessionTracker(interactor, context.system.scheduler)

  private val sessionCreator: SessionCreator = new SessionCreator(
    interactor, context.system.scheduler,
    self, repository, sessionConfig)

  private val heartBeatMonitor: ActorRef = context.actorOf(Props
  (new GenericSupervisor(Props(new HeartBeatMonitor(interactor, self)),
    s"HeartBeatMonitor-of-${self.path.name}")
  ), name = s"HeartBeatMonitor-of-${self.path.name}-Supervisor")

  private val providerConfigurations: ProviderConfigurations =
    readConfigurations(configuration)


  private var providerState: ProviderState = ProviderState(-1, -1, 0, 0, 0, SessionProviderStatus.SessionNotStarted)

  Logger.debug(s"actor: ${self.path.name}, providerConfigurations :$providerConfigurations")

  repository ! RetrieveLostSessionId

  private val scheduledRefresh: Cancellable =
    context.system.scheduler.schedule(
      0 seconds,
      1 minutes,
      self,
      RefreshSessionsIfNeeded)


  override def receive: Receive = {
    case message =>
      Logger.debug(s"actor: ${self.path.name}, " +
        s"Message: $message, state :$providerState")
      message match {
        case SessionCreationFailedAfterLotOfRetries(retries) =>
          Logger.error(s"Session creation failed after $retries retries," +
            s".Transitioning to dead state")
          providerState = providerState.copy(state = SessionProviderStatus.dead)
        case FailedToRetrieveLostSessionId =>
          sessionCreator.createNewSessionAndMonitor()
        case HereIsYourNewSession(id, creationTime) =>
          handleNewSession(id, creationTime)
        case SessionYouGaveIsDead(id) =>
          reactToDeadSessionNotification(id)
        case GetSessionId =>
          sender() ! {
            if (providerState.state == SessionProviderStatus.dead) {
              InvalidStateRestartRequired
            }
            else if (providerState.state != SessionProviderStatus.SessionNotStarted) {
              SessionId(providerState.activeSessionId)
            }
            else SessionNotAvailable
          }
        case SuccessfulRunIndication(id) =>
          if (id == providerState.activeSessionId)
            providerState = providerState.copy(totalSubSequentFailures = 0)
        case RequestSubmissionIndicator(id) =>
          if (id == providerState.activeSessionId)
            providerState = providerState.copy(totalRequests = providerState.totalRequests + 1)
          else if (id == providerState.secondarySessionId)
            Logger.warn(s"Inconsistent state, " +
              s"request submission to secondary session ${providerState.secondarySessionId}")
        case ErrorEncounteredIndication(id) =>
          if (id == providerState.activeSessionId) {
            providerState = providerState.copy(totalSubSequentFailures = providerState.totalSubSequentFailures + 1)
          }
        case RefreshSessionsIfNeeded =>
          if (providerState.state == SessionProviderStatus.dead) {
            Logger.error("Session provider is in dead state." +
              "Fix errors and  restart profiler agent to continue")
          }
          else if (providerState.state != SessionProviderStatus.SessionNotStarted) {
            heartBeatMonitor ! SendHeartBeat(providerState.activeSessionId)
            if (providerState.state != SessionProviderStatus.SwapStarted) swapSessionIfRequired()
          }
        case LostSessionId(id, creationTimeInMinutes) =>
          val checkSessionSate = interactor.getSessionState(id).map(
            state => {
              if (lostSessionStates.contains(state.state)) {
                repository ! DeletedSessionId(id)
                repository ! RetrieveLostSessionId
              }
              else {
                Logger.info(s"Reusing lost session $id")
                repository ! NewSessionId(id, creationTimeInMinutes)
                self ! HereIsYourNewSession(id, creationTimeInMinutes)
              }
            }
          )
          checkSessionSate onFailure {
            case error =>
              Logger.error(s"Error while checking state of lost session $id." +
                s" Trying for other session", error)
              repository ! DeletedSessionId(id)
              repository ! RetrieveLostSessionId
          }
        case NothingToConsume =>
          self ! FailedToRetrieveLostSessionId
        case unknowMessage =>
          Logger.error(s"Unknown message $unknowMessage ast actor SessionProvider")
      }
  }


  private def reactToDeadSessionNotification(id: Int) = {
    repository ! DeletedSessionId(id)
    if (id == providerState.activeSessionId && providerState.state != SessionProviderStatus.SessionNotStarted) {
      Logger.info(s"Session $id lost")
      interactor.deleteSession(id)
      if (providerState.state != SessionProviderStatus.SwapStarted) {
        providerState = providerState.copy(state = SessionProviderStatus.SessionNotStarted)
        sessionCreator.createNewSessionAndMonitor()
      }
      else {
        Logger.warn(s"Lost active session  $id during swapping")
      }
    }
    else if (id == providerState.secondarySessionId) {
      Logger.info(s"Secondary session $id lost")
    }
  }

  private def handleNewSession(id: Int, creationTime: Long) = {
    if (providerState.state == SessionProviderStatus.SessionNotStarted) {
      providerState = providerState.copy(activeSessionId = id,
        totalRequests = 0,
        totalSubSequentFailures = 0,
        sessionCreationTimeInMinutes = creationTime,
        state = SessionProviderStatus.SessionStarted)
    }
    else if (providerState.state == SessionProviderStatus.SwapStarted) {
      providerState = providerState.copy(
        secondarySessionId = providerState.activeSessionId,
        activeSessionId = id,
        totalRequests = 0,
        totalSubSequentFailures = 0,
        sessionCreationTimeInMinutes = creationTime,
        state = SessionProviderStatus.SessionStarted)
      repository ! DeletedSessionId(providerState.secondarySessionId)
      expiredSessionTracker.waitForSessionToDie(providerState.secondarySessionId, 20)
    }
    else {
      Logger.error(s"Unexpected behaviour , " +
        s"got a new session $id in state ${providerState.state}, killing it ")
      interactor.deleteSession(id)
    }
  }

  private def swapSessionIfRequired(): Unit = {
    val currentTimeInMinutes = System.currentTimeMillis() / numberOfMillisInMinutes
    val lifeTimeEnded = (currentTimeInMinutes - providerState.sessionCreationTimeInMinutes) > providerConfigurations.sessionLifetimeInMinutes
    val reachedMaximumAllowedFailures = providerState.totalSubSequentFailures > providerConfigurations.numberOfErrorsUntilSessionTerminated
    val sessionProcessedAllowedNumberOfQueries = providerState.totalRequests > providerConfigurations.sessionLifetimeInNumberOfRequests
    val sessionRefreshCondition = lifeTimeEnded || reachedMaximumAllowedFailures || sessionProcessedAllowedNumberOfQueries
    if (providerState.state == SessionProviderStatus.SessionStarted && sessionRefreshCondition) {
      if (reachedMaximumAllowedFailures)
        Logger.error(s"Swapping session ${providerState.activeSessionId} " +
          s"since it reached maximum allowed subsequent failures")
      else
        Logger.debug(s"Swapping session ${providerState.activeSessionId} , " +
          s"session life time : ${currentTimeInMinutes - providerState.sessionCreationTimeInMinutes}" +
          s", number of requests handled : ${providerState.totalRequests}")
      providerState = providerState.copy(state = SessionProviderStatus.SwapStarted)
      sessionCreator.createNewSessionAndMonitor()
    }
    else if (sessionRefreshCondition) {
      Logger.warn(s"Session refresh condition occurred too early in state ${providerState.state}")
    }
  }

}
