/*
 HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES


  (c) 2016-2018 Hortonworks, Inc. All rights reserved.

  This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
  Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
  to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
  properly licensed third party, you do not have any rights to this code.

  If this code is provided to you under the terms of the AGPLv3:
  (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
  (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
    LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
  (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
    FROM OR RELATED TO THE CODE; AND
  (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
    DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
    DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
    OR LOSS OR CORRUPTION OF DATA.
*/
package utils

import org.scalatest.mockito.MockitoSugar
import org.scalatestplus.play.PlaySpec
import play.api.test.{DefaultAwaitTimeout, FutureAwaits}
import play.api.Configuration
import org.mockito.Mockito._
import play.api.libs.json.Json
import play.api.libs.ws.{WSClient, WSRequest, WSResponse}

import scala.concurrent.Future

class YarnInteractorSpec extends PlaySpec with FutureAwaits with DefaultAwaitTimeout with MockitoSugar{
  val configMock = mock[Configuration]
  val wsClientMock = mock[WSClient]
  val wsResponseMock = mock[WSResponse]
  val wsRequestMock = mock[WSRequest]

  val jsResponse = Json.obj("scheduler" ->
    Json.obj("schedulerInfo" ->
      Json.obj("type" ->
        "random", "capacity" -> 12.23,
        "usedCapacity" -> 21332,
        "maxCapacity" -> 345,
        "queueName" -> "default",
        "queues" -> Json.obj())))

  when(configMock.getString("yarn.rm.url")).thenReturn(Some("localhost"))
  when(configMock.getBoolean("dpprofiler.secured")).thenReturn(Some(false))
  when(wsResponseMock.status).thenReturn(200)
  when(wsResponseMock.json).thenReturn(jsResponse)

  import scala.concurrent.ExecutionContext.Implicits.global
  val yarnInteractor = new YarnInteractor(configMock, wsClientMock)

  when(wsClientMock.url(s"http://localhost/ws/v1/cluster/scheduler")).thenReturn(wsRequestMock)
  when(wsRequestMock.withHeaders("Content-Type" -> "application/json", "X-Requested-By" -> "profiler")).thenReturn(wsRequestMock)
  when(wsRequestMock.get()).thenReturn(Future.successful(wsResponseMock))

  "Yarn interactor " must {
    " be able to extract queue names from the Resource Manager response " in {
      val result = yarnInteractor.getYarnQueueSet()
      await(result)
      val resultQueues = result.value.get.get
      assert(resultQueues.contains("default"))
    }
  }
}
