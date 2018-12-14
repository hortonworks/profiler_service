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
package auth

import org.scalatest.mockito.MockitoSugar
import org.mockito.Mockito._
import org.scalatest.WordSpecLike
import play.api.mvc._
import filters.LoggingUserNameFilter
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

class LoggingUserNameFilterTest extends WordSpecLike with MockitoSugar{
    import scala.concurrent.ExecutionContext.Implicits.global
    implicit val actorSystem = ActorSystem("LoggingUserNameFilterTest")
    implicit val materializer = ActorMaterializer()
    val loggingUserNameFilter = new LoggingUserNameFilter()

    "Logging user name filter should " must {
      "extract the username correctly from the request header" in {
        val requestMock = mock[RequestHeader]
        when(requestMock.getQueryString("user.name")).thenReturn(Some("dummy"))
        val username = loggingUserNameFilter.extractAndLogUserName(requestMock)
        assert(username == "dummy")
      }

      "return unknown when no user name is present" in {
        val requestMock = mock[RequestHeader]
        when(requestMock.getQueryString("user.name")).thenReturn(None)
        val username = loggingUserNameFilter.extractAndLogUserName(requestMock)
        assert(username == "unknown")
      }
    }
}
