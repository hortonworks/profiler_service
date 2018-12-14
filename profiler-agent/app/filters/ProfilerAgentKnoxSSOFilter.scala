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
package filters

import java.security.cert.CertificateFactory

import akka.stream.Materializer
import play.api.{Configuration, Logger}
import play.api.mvc.{RequestHeader, Result, Results}

import scala.concurrent.{ExecutionContext, Future}
import com.nimbusds.jose.crypto.RSASSAVerifier
import com.nimbusds.jwt.SignedJWT


class ProfilerAgentKnoxSSOFilter (implicit val mat: Materializer, ec: ExecutionContext, config: Configuration){
  private val knoxSSOHelper = new KnoxSSOAuthHelper()
  private val publicKeyVerifier = getRSASSAVerifier()

  private def getRSASSAVerifier():Future[RSASSAVerifier] = {
    val knoxPublicKeyString = config.getString("dpprofiler.sso.knox.public.key").getOrElse("")
    val certificateFactory = CertificateFactory.getInstance("X.509")
    val knoxPublicKey = knoxSSOHelper.parseRSAPublicKey(knoxPublicKeyString, certificateFactory)
    knoxPublicKey.flatMap{
      key => {
        Future.successful(new RSASSAVerifier(key))
      }
    }
  }

  def performSSOAuthentication(nextFilter: RequestHeader => Future[Result], requestHeader: RequestHeader) = {
      val ssoCookieName = config.getString("dpprofiler.sso.knox.cookiename").getOrElse("hadoop-jwt")
      publicKeyVerifier.flatMap{
          key => {
            val ssoCookie = requestHeader.cookies.get(ssoCookieName)
            ssoCookie match {
              case Some(cookie) => {
                val ssoCookieValue = cookie.value
                val signedJWT = SignedJWT.parse(ssoCookieValue)
                knoxSSOHelper.validateSSOToken(signedJWT, key, ec).
                  flatMap(isValid =>
                    isValid match {
                      case true => {
                        Logger.info(s"Knox SSO authentication successful")
                        nextFilter(requestHeader)
                      }
                      case false => {
                        Logger.error(s"SSO token in request is not valid. Knox SSO Authentication failed")
                        Future.successful(Results.Unauthorized)
                      }
                  }
                )
              }
              case None => {
                Logger.warn(s"No cookie with name $ssoCookieName found in the request. Knox SSO Authentication failed")
                Future.successful(Results.Unauthorized)
              }
            }
          }
        } recoverWith  {
          case th:Throwable => {
              Logger.error(s"Knox SSO authentication failed with the exception ${th.getMessage}")
              Future.successful(Results.Unauthorized)
          }
      }
  }
}
