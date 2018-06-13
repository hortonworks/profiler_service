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

import livy.session.models.LivyResponses.{StateOfSession, StateOfStatement}
import play.api.Configuration

object SessionProviderHelper {

  object SessionProviderStatus extends Enumeration {
    type SessionProviderStatus = Value
    val SessionNotStarted, SessionStarted, SwapStarted, dead = Value
  }

  case class ProviderState(activeSessionId: Int, secondarySessionId: Int,
                           totalRequests: Int, totalSubSequentFailures: Int,
                           sessionCreationTimeInMinutes: Long, state: SessionProviderStatus.SessionProviderStatus)

  case class ProviderConfigurations(sessionLifetimeInMinutes: Int
                                    , sessionLifetimeInNumberOfRequests: Int
                                    , numberOfErrorsUntilSessionTerminated: Int)


  val lostSessionStates = Set(StateOfSession.dead, StateOfSession.error, StateOfSession.shutting_down)

  val activeSessionStates = Set(StateOfSession.busy, StateOfSession.idle)

  val startingUpSessionStates = Set(StateOfSession.not_started, StateOfSession.starting)

  val activeStatementStates = Set(StateOfStatement.running, StateOfStatement.waiting)

  val numberOfMillisInMinutes: Long = 1000 * 60


  def readConfigurations(configuration: Configuration): ProviderConfigurations = {
    val sessionLifetimeInMinutes = configuration.
      getInt("livy.session.lifetime.minutes")
      .getOrElse(120)
    val sessionLifetimeInNumberOfRequests = configuration
      .getInt("livy.session.lifetime.requests")
      .getOrElse(500)
    val numberOfErrorsUntilSessionTerminated = configuration
      .getInt("livy.session.max.errors")
      .getOrElse(25)
    ProviderConfigurations(sessionLifetimeInMinutes,
      sessionLifetimeInNumberOfRequests, numberOfErrorsUntilSessionTerminated)
  }


}