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
import org.scalatest.{BeforeAndAfterAll, WordSpecLike}
import play.api.mvc._
import play.api.Configuration
import filters.SpnegoAuthenticationFilter
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.scalatest.mockito.MockitoSugar
import play.api.test.{DefaultAwaitTimeout, FutureAwaits}

import scala.concurrent.Future

  class SpnegoAuthenticationFilterTest extends WordSpecLike with MockitoSugar with FutureAwaits
    with DefaultAwaitTimeout with BeforeAndAfterAll{
    implicit val actorSystem = ActorSystem("SpnegoAuthenticationFilterTest")
    implicit val executionContext = actorSystem.dispatcher
    implicit val materializer = ActorMaterializer()
    val nonSecureConfigMock = getMockConfigNonSecureMode()
    val secureConfigMock = getMockConfigSecureMode()
    val requestHeaderMock = mock[RequestHeader]
    val spnegoAuthenticator = mock[SpnegoAuthenticator]
    val sampleNextFunction = mock[RequestHeader => Future[Result]]
    val sampleResult = mock[Result]

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  def getMockConfigNonSecureMode() = {
    val mockConfig = mock[Configuration]
    when(mockConfig.getBoolean("credential.store.enabled")).thenReturn(Some(false))
    when(mockConfig.getString("credential.provider.path")).thenReturn(Some("/somepath"))
    when(mockConfig.getString("dpprofiler.spnego.signature.secret")).thenReturn(Some("somesecret"))
    when(mockConfig.getBoolean("dpprofiler.secured")).thenReturn(Some(false))
    when(mockConfig.getBoolean("dpprofiler.sso.knox.enabled")).thenReturn(Some(false))
    mockConfig
  }

  def getMockConfigSecureMode() = {
    val mockConfig = mock[Configuration]
    when(mockConfig.getBoolean("credential.store.enabled")).thenReturn(Some(false))
    when(mockConfig.getString("credential.provider.path")).thenReturn(Some("/somepath"))
    when(mockConfig.getBoolean("dpprofiler.secured")).thenReturn(Some(true))
    when(mockConfig.getString("dpprofiler.spnego.kerberos.principal")).thenReturn(Some("HTTP/_HOST@REALM.COM"))
    when(mockConfig.getString("dpprofiler.spnego.kerberos.keytab")).thenReturn(Some("somekeytab"))
    when(mockConfig.getBoolean("http.spnego.kerberos.debug")).thenReturn(Some(true))
    when(mockConfig.getString("spnego.cookie.domain")).thenReturn(Some(""))
    when(mockConfig.getString("spnego.cookie.path")).thenReturn(Some(""))
    when(mockConfig.getLong("spnego.token.validity")).thenReturn(Some(0L))
    when(mockConfig.getString("dpprofiler.spnego.signature.secret")).thenReturn(Some("somesecret"))
    when(mockConfig.getString("dpprofiler.spnego.cookie.name")).thenReturn(Some("somename"))
    when(mockConfig.getBoolean("dpprofiler.sso.knox.enabled")).thenReturn(Some(false))
    mockConfig
  }

  def contructCookie(token:Token) = {
    val tokens = new Tokens(1000000L, "dpprofilersecret".getBytes(UTF_8))
    new Cookie("dpprofiler.spnego.cookie", tokens.serialize(token))
  }

  "Spnego authentication filter " must {

    " invoke the filter when dpprofiler.secured is set to true " in {
      implicit val config = secureConfigMock
      val spnegoAuthenticationFilter = new SpnegoAuthenticationFilter()
      when(spnegoAuthenticator.apply(requestHeaderMock)).thenReturn(
        Left(new AuthenticationMessageWithToken("auth failed", None)))
      when(sampleNextFunction(requestHeaderMock)).thenReturn(Future.successful(sampleResult))
      val result = spnegoAuthenticationFilter.performSpnegoAuthentication(
        sampleNextFunction, requestHeaderMock, spnegoAuthenticator)
      assert(result.value.get.get != sampleResult)
    }

    " return 401 Unauthorized when authentication fails " in {
      implicit val config = secureConfigMock
      val spnegoAuthenticationFilter = new SpnegoAuthenticationFilter()
      when(spnegoAuthenticator.apply(requestHeaderMock)).
        thenReturn(Left(new AuthenticationMessageWithToken("auth failed", None)))
      val result = spnegoAuthenticationFilter.performSpnegoAuthentication(
        sampleNextFunction, requestHeaderMock, spnegoAuthenticator)
      assert(result.value.get.get == Results.Unauthorized)
    }

    " return 401 Unauthorized with Negotiate challenge when a token is returned " in {
      implicit val config = secureConfigMock
      val spnegoAuthenticationFilter = new SpnegoAuthenticationFilter()
      val tokenString = "sampletoken"
      when(spnegoAuthenticator.apply(requestHeaderMock)).
        thenReturn(Left(new AuthenticationMessageWithToken("auth failed", Some(tokenString))))
      val result = spnegoAuthenticationFilter.performSpnegoAuthentication(
        sampleNextFunction, requestHeaderMock, spnegoAuthenticator)
      assert(result.value.get.get == Results.Unauthorized.withHeaders(("WWW-Authenticate",
        "Negotiate " + tokenString)))
    }

    " return updated Result with cookie on successful authentication " in {
      val token = new Token("sampleprincipal", 100000L)
      implicit val config = secureConfigMock
      val spnegoAuthenticationFilter = new SpnegoAuthenticationFilter()
      when(spnegoAuthenticator.apply(requestHeaderMock)).thenReturn(Right(token))
      when(sampleNextFunction(requestHeaderMock)).thenReturn(Future.successful(sampleResult))
      val mockResultWithCookie = sampleNextFunction(requestHeaderMock).map(result =>
        result.withCookies(contructCookie(token)))
      val authResultWithCookie = spnegoAuthenticationFilter.performSpnegoAuthentication(sampleNextFunction,
        requestHeaderMock, spnegoAuthenticator)

      val expectedResult = await(mockResultWithCookie)
      val actualResult = await(authResultWithCookie)
      assert(expectedResult == actualResult)
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()
    actorSystem.terminate()
  }

}