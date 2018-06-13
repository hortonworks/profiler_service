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

import java.nio.charset.StandardCharsets.UTF_8

import org.mockito.Mockito._
import org.scalatest.WordSpecLike
import play.api.mvc._
import play.api.Configuration
import filters.SpnegoAuthenticationFilter
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.scalatest.mockito.MockitoSugar
import org.scalatest.Matchers._

class SpnegoAuthenticationFilterTest extends WordSpecLike with MockitoSugar{
  implicit val executionContext = scala.concurrent.ExecutionContext.Implicits.global
  implicit val actorSystem = ActorSystem("SpnegoAuthenticationFilterTest")
  implicit val materializer = ActorMaterializer()
  implicit val config = mock[Configuration]
  val requestHeaderMock = mock[RequestHeader]
  val spnegoAuthenticator = mock[SpnegoAuthenticator]
  val sampleResult = mock[Result]
  val spnegoAuthenticationFilter = new SpnegoAuthenticationFilter()

  def contructCookie(token:Token) = {
    val tokens = new Tokens(1000000L, "dpprofilersecret".getBytes(UTF_8))
    new Cookie("dpprofiler.spnego.cookie", tokens.serialize(token))
  }


  "Spnego authentication filter " must {
    " not invoke the filter when dpprofiler.secured is set to false " in {
      when(config.getBoolean("dpprofiler.secured")).thenReturn(Some(false))
      val returnedResult = spnegoAuthenticationFilter.
        performSpnegoAuthentication(sampleResult, requestHeaderMock, spnegoAuthenticator)
      assert(returnedResult == sampleResult)
    }

    " invoke the filter when dpprofiler.secured is set to true " in {
      when(config.getBoolean("dpprofiler.secured")).thenReturn(Some(true))
      when(spnegoAuthenticator.apply(requestHeaderMock)).
        thenReturn(Left(new AuthenticationMessageWithToken("error", None)))
      val result = spnegoAuthenticationFilter.performSpnegoAuthentication(sampleResult,
        requestHeaderMock, spnegoAuthenticator)
      assert(result != sampleResult)
    }

    " return 401 Unauthorized when authentication fails " in {
      when(config.getBoolean("dpprofiler.secured")).
        thenReturn(Some(true))
      when(spnegoAuthenticator.apply(requestHeaderMock)).
        thenReturn(Left(new AuthenticationMessageWithToken("auth failed", None)))
      val authResult = spnegoAuthenticationFilter.
        performSpnegoAuthentication(sampleResult, requestHeaderMock, spnegoAuthenticator)
      assert(authResult == Results.Unauthorized)
    }

    " return 401 Unauthorized with Negotiate challenge when a token is returned " in {
      when(config.getBoolean("dpprofiler.secured")).thenReturn(Some(true))
      val tokenString = "sampletoken"
      when(spnegoAuthenticator.apply(requestHeaderMock)).
        thenReturn(Left(new AuthenticationMessageWithToken("auth failed", Some(tokenString))))
      val authResult = spnegoAuthenticationFilter.
        performSpnegoAuthentication(sampleResult, requestHeaderMock, spnegoAuthenticator)
      assert(authResult == Results.Unauthorized.withHeaders(("WWW-Authenticate", "Negotiate " + tokenString)))
    }

    " return updated response on successful authentication " in {
      val token = new Token("sampleprincipal", 100000L)
      when(config.getBoolean("dpprofiler.secured")).thenReturn(Some(true))
      when(config.getString("dpprofiler.spnego.signature.secret")).thenReturn(Some("dpprofilersecret"))
      when(config.getLong("spnego.token.validity")).thenReturn(Some(1000000L))
      when(config.getString("dpprofiler.spnego.cookie.name")).thenReturn(Some("dpprofiler.spnego.cookie"))
      when(spnegoAuthenticator.apply(requestHeaderMock)).thenReturn(Right(token))
      val newResult = mock[Result]
      when(sampleResult.withCookies(contructCookie(token))).thenReturn(newResult)
      val authResult = spnegoAuthenticationFilter.performSpnegoAuthentication(sampleResult, requestHeaderMock, spnegoAuthenticator)
      assert(authResult == newResult)
    }
  }
}
