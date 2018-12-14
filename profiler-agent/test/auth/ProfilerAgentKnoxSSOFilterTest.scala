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

import org.mockito.Mockito._
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import filters.ProfilerAgentKnoxSSOFilter
import filters.KnoxSSOAuthHelper
import org.scalatest.{BeforeAndAfterAll, WordSpecLike}
import org.scalatest.mockito.MockitoSugar
import play.api.mvc.{RequestHeader, Result, Cookies}
import play.api.mvc.Cookie
import play.api.test.{DefaultAwaitTimeout, FutureAwaits}
import play.api.Configuration
import com.nimbusds.jose.crypto.RSASSAVerifier
import com.nimbusds.jwt.SignedJWT

import scala.concurrent.Future

class ProfilerAgentKnoxSSOFilterTest extends WordSpecLike with MockitoSugar with FutureAwaits
  with DefaultAwaitTimeout with BeforeAndAfterAll{

  implicit val actorSystem = ActorSystem("ProfilerAgentKnoxSSOFilterTest")
  implicit val executionContext = actorSystem.dispatcher
  implicit val materializer = ActorMaterializer()
  val requestHeaderMock = mock[RequestHeader]
  val updatedRequestHeaderMock = mock[RequestHeader]
  val sampleNextFunction = mock[RequestHeader => Future[Result]]
  val sampleResult = mock[Result]
  val mockConfig = mock[Configuration]
  val knoxSSOAuthHelperMock = mock[KnoxSSOAuthHelper]
  val signedJwtMock = mock[SignedJWT]
  val rsaVerifierMock = mock[RSASSAVerifier]
  val cookiesMock = mock[Cookies]

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    actorSystem.terminate()
  }


  "ProfilerAgentKnoxSSOFilterTest filter " must {

    " invoke SSO authentication when SSO is enabled " in {
      when(mockConfig.getString("dpprofiler.sso.knox.cookiename")).thenReturn(Some("hadoop-jwt"))
      when(mockConfig.getString("dpprofiler.sso.knox.public.key")).thenReturn(Some("randomkey"))
      implicit val config = mockConfig
      val knoxSSOFilter = new ProfilerAgentKnoxSSOFilter()
      val authResult = knoxSSOFilter.performSSOAuthentication(sampleNextFunction, requestHeaderMock)
      await(authResult)
      assert(authResult.value.get.get.header.status == 401)
    }

    " fail Knox SSO authentication when no hadoop-jwt cookie is found in request " in {
      when(mockConfig.getString("dpprofiler.sso.knox.cookiename")).thenReturn(Some("hadoop-jwt"))
      when(mockConfig.getString("dpprofiler.sso.knox.public.key")).thenReturn(Utils.getValidRSAPublicKey())
      when(requestHeaderMock.cookies).thenReturn(cookiesMock)
      when(cookiesMock.get("hadoop-jwt")).thenReturn(None)
      implicit val config = mockConfig
      val knoxSSOFilter = new ProfilerAgentKnoxSSOFilter()
      val authResult = knoxSSOFilter.performSSOAuthentication(sampleNextFunction, requestHeaderMock)
      await(authResult)
      assert(authResult.value.get.get.header.status == 401)
    }

    " fail Knox SSO authentication when JWT cookie validation fails " in {
      when(mockConfig.getString("dpprofiler.sso.knox.cookiename")).thenReturn(Some("hadoop-jwt"))
      when(mockConfig.getString("dpprofiler.sso.knox.public.key")).thenReturn(Utils.getValidRSAPublicKey())

      when(requestHeaderMock.cookies).thenReturn(cookiesMock)
      val invalidCookieMock = mock[Cookie]
      when(invalidCookieMock.name).thenReturn("hadoop-jwt")
      when(invalidCookieMock.value).thenReturn("somerandomvalue")
      when(cookiesMock.get("hadoop-jwt")).thenReturn(Some(invalidCookieMock))
      implicit val config = mockConfig
      val knoxSSOFilter = new ProfilerAgentKnoxSSOFilter()
      val authResult = knoxSSOFilter.performSSOAuthentication(sampleNextFunction, requestHeaderMock)
      await(authResult)
      assert(authResult.value.get.get.header.status == 401)
    }

    " perform Knox SSO authentication successfully when JWT cookie validation succeeds " in {
      when(mockConfig.getString("dpprofiler.sso.knox.cookiename")).thenReturn(Some("hadoop-jwt"))
      when(mockConfig.getString("dpprofiler.sso.knox.public.key")).thenReturn(Utils.getValidRSAPublicKey())

      when(requestHeaderMock.cookies).thenReturn(cookiesMock)
      val validCookieMock = mock[Cookie]
      when(validCookieMock.name).thenReturn("hadoop-jwt")
      when(validCookieMock.value).thenReturn(Utils.getValidHadoopJwtToken())
      when(cookiesMock.get("hadoop-jwt")).thenReturn(Some(validCookieMock))
      when(sampleNextFunction(requestHeaderMock)).thenReturn(Future.successful(sampleResult))
      implicit val config = mockConfig
      val knoxSSOFilter = new ProfilerAgentKnoxSSOFilter()
      val authResult = knoxSSOFilter.performSSOAuthentication(sampleNextFunction, requestHeaderMock)
      await(authResult)
      assert(authResult.value.get.get== sampleResult)
    }
  }
}
