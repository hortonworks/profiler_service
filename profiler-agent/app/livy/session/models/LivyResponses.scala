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

package livy.session.models

import livy.session.models.LivyResponses.StateOfBatchJob.StateOfBatchJob
import livy.session.models.LivyResponses.StateOfSession.StateOfSession
import livy.session.models.LivyResponses.StateOfStatement.StateOfStatement
import play.api.libs.json.JsValue

object LivyResponses {

  object StateOfSession extends Enumeration {
    type StateOfSession = Value
    val not_started, starting, idle, busy, shutting_down, error, dead, success = Value
  }

  object StateOfStatement extends Enumeration {
    type StateOfStatement = Value
    val waiting, running, available, error, cancelling, cancelled = Value
  }


  object StateOfBatchJob extends Enumeration {
    type StateOfBatchJob = Value
    val not_started, starting, recovering, idle, running, busy, shutting_down, error, dead, success, not_found = Value
  }


  case class Session(id: Int, appId: JsValue, owner: JsValue, proxyUser: String, kind: String, state: StateOfSession)

  case class SessionState(id: Int, state: StateOfSession)

  abstract class StatementOutPut {
    def status: String
  }

  case class SuccessfulStatementOutPut(status: String, execution_count: Int, data: JsValue) extends StatementOutPut

  case class ErrorStatementOutput(status: String, execution_count: Int,
                                  traceback: List[String], ename: String, evalue: String) extends StatementOutPut

  case class Statement(id: Int, code: String, state: StateOfStatement, output: Option[StatementOutPut], progress: Float)

  case class Statements(statements: List[Statement])

  case class Batch(id: Int, appId: Option[String], appInfo: JsValue, log: List[String], state: StateOfBatchJob)

  case class BatchLog(id: Int, from: Int, size: Int, log: List[String])

  case class BatchState(id: Int, state: String)

  case class Batches(from: Int, total: Int, sessions: List[Batch])

}
