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

package livy.session.models.parsers

import livy.session.models.LivyResponses.StateOfBatchJob.StateOfBatchJob
import livy.session.models.LivyResponses.StateOfSession.StateOfSession
import livy.session.models.LivyResponses.StateOfStatement.StateOfStatement
import livy.session.models.LivyResponses._
import play.api.libs.functional.syntax._
import play.api.libs.json._

object ResponseParsers {
  private implicit val stateOfSessionFormat: Format[StateOfSession] = new Format[StateOfSession] {
    def reads(json: JsValue) = JsSuccess(StateOfSession.withName(json.as[String]))

    def writes(myEnum: StateOfSession) = JsString(myEnum.toString)
  }

  private implicit val stateOfStatementFormat: Format[StateOfStatement] = new Format[StateOfStatement] {
    def reads(json: JsValue) = JsSuccess(StateOfStatement.withName(json.as[String]))

    def writes(myEnum: StateOfStatement) = JsString(myEnum.toString)
  }


  private implicit val stateOfBatchJobFormat: Format[StateOfBatchJob] = new Format[StateOfBatchJob] {
    def reads(json: JsValue) = JsSuccess(StateOfBatchJob.withName(json.as[String]))

    def writes(myEnum: StateOfBatchJob) = JsString(myEnum.toString)
  }

  private implicit val successfulStatementOutPutFormat: Format[SuccessfulStatementOutPut] = Json.format[SuccessfulStatementOutPut]
  private implicit val errorStatementOutputFormat: Format[ErrorStatementOutput] = Json.format[ErrorStatementOutput]


  implicit val statementOutputReads: Reads[StatementOutPut] = new Reads[StatementOutPut] {
    override def reads(json: JsValue): JsResult[StatementOutPut] =
      (json \ "status").as[String] match {
        case "error" => json.validate[ErrorStatementOutput]
        case "ok" => json.validate[SuccessfulStatementOutPut]
        case x => JsError(s"Invalid status $x")
      }
  }

  implicit val statementReads: Reads[Statement] = ((JsPath \ "id").read[Int] and
    (JsPath \ "code").read[String] and
    (JsPath \ "state").read[StateOfStatement] and
    (JsPath \ "output").read[JsValue] and
    (JsPath \ "progress").read[Float]) (
    (id, code, state, outputJson, progress) => {
      state match {
        case StateOfStatement.available => Statement(id, code, state, Some(outputJson.as[StatementOutPut]), progress)
        case _ => Statement(id, code, state, None, progress)
      }
    }
  )


  implicit val sessionFormat: Format[Session] = Json.format[Session]
  implicit val sessionStateFormat: Format[SessionState] = Json.format[SessionState]
  implicit val statementsFormat: Reads[Statements] = Json.reads[Statements]

  implicit val batchFormat: Reads[Batch] = Json.reads[Batch]
  implicit val batchLogFormat: Reads[BatchLog] = Json.reads[BatchLog]
  implicit val batchStateFormat: Reads[BatchState] = Json.reads[BatchState]
  implicit val batchesFormat: Reads[Batches] = Json.reads[Batches]
}
