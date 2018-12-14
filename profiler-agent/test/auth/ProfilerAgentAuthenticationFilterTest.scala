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
package auth

import org.mockito.Mockito._
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.scalatest.{BeforeAndAfterAll, WordSpecLike}
import org.scalatest.mockito.MockitoSugar
import play.api.mvc.{RequestHeader, Result, Cookies}
import play.api.mvc.Cookie
import play.api.test.{DefaultAwaitTimeout, FutureAwaits}
import play.api.Configuration
import filters.ProfilerAgentAuthenticationFilter

import scala.concurrent.Future

class ProfilerAgentAuthenticationFilterTest extends WordSpecLike with MockitoSugar with FutureAwaits
  with DefaultAwaitTimeout with BeforeAndAfterAll{

  implicit val actorSystem = ActorSystem("ProfilerAgentAuthenticationFilterTest")
  implicit val executionContext = actorSystem.dispatcher
  implicit val materializer = ActorMaterializer()
  val requestHeaderMock = mock[RequestHeader]
  val updatedRequestHeaderMock = mock[RequestHeader]
  val sampleNextFunction = mock[RequestHeader => Future[Result]]
  val sampleResult = mock[Result]
  val mockConfig = mock[Configuration]
  val cookiesMock = mock[Cookies]

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    actorSystem.terminate()
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
    when(mockConfig.getString("dpprofiler.sso.knox.cookiename")).thenReturn(Some("hadoop-jwt"))
    mockConfig
  }

  "ProfilerAgentAuthenticationFilterTest " must {
    " skip authentication filters when sso and kerberos are disabled " in {
      when(mockConfig.getBoolean("dpprofiler.secured")).thenReturn(Some(false))
      when(mockConfig.getBoolean("dpprofiler.sso.knox.enabled")).thenReturn(Some(false))
      when(mockConfig.getString("dpprofiler.sso.knox.cookiename")).thenReturn(Some("hadoop-jwt"))
      when(sampleNextFunction(requestHeaderMock)).thenReturn(Future.successful(sampleResult))
      implicit val config = mockConfig
      val profilerAgentAuthenticationFilter = new ProfilerAgentAuthenticationFilter()
      val result = profilerAgentAuthenticationFilter.apply(sampleNextFunction)(requestHeaderMock)
      await(result)
      assert(result.value.get.get == sampleResult)
    }

    " attempt Knox SSO filter when SSO is enabled and hadoop-jwt cookie is present in request header " in {
      when(mockConfig.getBoolean("dpprofiler.secured")).thenReturn(Some(false))
      when(mockConfig.getBoolean("dpprofiler.sso.knox.enabled")).thenReturn(Some(true))
      when(mockConfig.getString("dpprofiler.sso.knox.cookiename")).thenReturn(Some("hadoop-jwt"))
      when(mockConfig.getString("dpprofiler.sso.knox.public.key")).thenReturn(Some(""))
      implicit val config = mockConfig
      when(requestHeaderMock.cookies).thenReturn(cookiesMock)
      val validCookieMock = mock[Cookie]
      when(validCookieMock.name).thenReturn("hadoop-jwt")
      when(cookiesMock.get("hadoop-jwt")).thenReturn(Some(validCookieMock))
      val profilerAgentAuthenticationFilter = new ProfilerAgentAuthenticationFilter()
      val result = profilerAgentAuthenticationFilter.apply(sampleNextFunction)(requestHeaderMock)
      await(result)
      assert(result.value.get.get.header.status == 401)

    }

    " attempt SPNEGO authentication filter when SSO is disabled and kerberos is enabled " in {
      implicit val config = getMockConfigSecureMode()
      val profilerAgentAuthenticationFilter = new ProfilerAgentAuthenticationFilter()
      intercept[NullPointerException]{
        await(profilerAgentAuthenticationFilter.apply(sampleNextFunction)(requestHeaderMock))
      }
    }
  }
}
