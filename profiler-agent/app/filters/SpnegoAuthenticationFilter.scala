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

import java.nio.charset.StandardCharsets.UTF_8
import javax.inject.Inject

import akka.stream.Materializer
import auth.{SpnegoAuthenticator, Token, Tokens}
import play.api.mvc._
import play.api.{Configuration, Logger}

import scala.concurrent.{ExecutionContext, Future}
import java.net.InetAddress
import java.net.UnknownHostException
import com.hortonworks.dataplane.profilers.commons.security.ProfilerCredentialProvider


class SpnegoAuthenticationFilter (implicit val mat: Materializer, ec: ExecutionContext, config: Configuration){

  private val passwordEncrytionEnabled = config.getBoolean("credential.store.enabled").getOrElse(false)
  private val credentialFile = config.getString("credential.provider.path").
    getOrElse("jceks://file/etc/profiler_agent/conf/dpprofiler-config.jceks")
  val spnegoAuthenticator = contructSpnegoAuthenticator()

  private def getDecryptedSignatureSecret() = {
    if(passwordEncrytionEnabled){
      val credentialProvider = new ProfilerCredentialProvider(credentialFile)
      val decryptedSignatureSecret = credentialProvider.resolveAlias("dpprofiler.spnego.signature.secret")
      decryptedSignatureSecret
    }else{
        config.getString("dpprofiler.spnego.signature.secret").getOrElse("dpprofilersecret")
    }
  }

  private def contructCookie(token: Token) = {
    val tokenValidity = config.getLong("spnego.token.validity").getOrElse(1000000L)
    val cookieName = config.getString("dpprofiler.spnego.cookie.name").getOrElse("dpprofiler.spnego.cookie")
    val spnegoSignatureSecret = getDecryptedSignatureSecret()
    val tokens = new Tokens(tokenValidity, spnegoSignatureSecret.getBytes(UTF_8))
    new Cookie(cookieName, tokens.serialize(token))
  }

  def getBindAddress(): String = {
    val bindAddress = try {
      InetAddress.getLocalHost.getHostName
    } catch {
      case e: UnknownHostException => null
    }
    bindAddress
  }

  def getPrincipalComponents(principal: Option[String]): List[String] = {
    principal match {
      case Some(p) => p.split("[/@]").toList
      case _ => List()
    }
  }

  def getActualPrincipal(configPrincipal: Option[String], fqdn: String): String = {
    val principalComponents = getPrincipalComponents(configPrincipal)
    val actualPrincipal =
      if (principalComponents.nonEmpty &&
        principalComponents.size == 3 &&
        principalComponents(1).equals("_HOST")) {
        principalComponents(0) + "/" + fqdn.toLowerCase() + "@" + principalComponents(2)
      } else {
        ""
      }
    actualPrincipal
  }

  def contructSpnegoAuthenticator(): SpnegoAuthenticator = {
      val bindAddress = getBindAddress()
      val fullyQualifiedName =
        if (bindAddress == null || bindAddress.isEmpty || bindAddress.equals("0.0.0.0")) {
          InetAddress.getLocalHost.getCanonicalHostName
        } else {
          bindAddress
        }

      val configPrincipal = config.getString("dpprofiler.spnego.kerberos.principal")
      val configPrincipalValue = configPrincipal.getOrElse("HTTP/_HOST@REALM.COM")
      Logger.debug(s"SPNEGO principal from configs => ${configPrincipalValue}")
      val principal = getActualPrincipal(configPrincipal, fullyQualifiedName)
      Logger.debug(s"Resolved SPNEGO principal => ${principal}")

      val keytab = config.getString("dpprofiler.spnego.kerberos.keytab").getOrElse("dummy")
      val debug = config.getBoolean("http.spnego.kerberos.debug").getOrElse(true)
      val domain = config.getString("spnego.cookie.domain")
      val path = config.getString("spnego.cookie.path")
      val tokenValidity = config.getLong("spnego.token.validity").getOrElse(1000000L)
      val cookieName = config.getString("dpprofiler.spnego.cookie.name").getOrElse("dpprofiler.spnego.cookie")
      val spnegoSignatureSecret = getDecryptedSignatureSecret()

      Logger.debug(s"principal ${principal}")
      Logger.debug(s"keytab ${keytab}")
      Logger.debug(s"debug ${debug}")

      val tokens = new Tokens(tokenValidity, spnegoSignatureSecret.getBytes(UTF_8))
      new SpnegoAuthenticator(principal, keytab, debug, domain, path, tokens, cookieName)
  }

  def performSpnegoAuthentication(nextFilter: RequestHeader => Future[Result], requestHeader: RequestHeader, spnego:SpnegoAuthenticator) = {
    Logger.debug("Kerberos is enabled for profiler agent. Attempting SPNEGO authentication")
    spnego.apply(requestHeader) match {
      case Left(e) => {
        e.token match {
          case Some(t) => {
            Logger.warn("Authentication unsuccessful. Sending Negotiate challenge back to client")
            Future.successful(Results.Unauthorized.withHeaders(("WWW-Authenticate", "Negotiate " + t)))
          }
          case _ => {
            Logger.error(s"Authentication failed with exception ${e.msg}")
            Future.successful(Results.Unauthorized)
          }
        }
      }
      case Right(token: Token) => {
        Logger.debug(s"DP profiler SPNEGO authentication successful. " +
          s"Authenticator returned token ${token.principal} expiration ${token.expiration.toString} ")
        nextFilter(requestHeader).map(result => result.withCookies(contructCookie(token)))
      }
    }
  }
}
