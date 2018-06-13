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

import java.io.IOException
import java.security.{PrivilegedAction, PrivilegedActionException, PrivilegedExceptionAction}
import javax.security.auth.Subject
import javax.security.auth.login.LoginContext
import javax.security.auth.kerberos.KerberosPrincipal
import scala.collection.JavaConverters._
import org.ietf.jgss.{GSSCredential, GSSManager}
import org.apache.commons.codec.binary.Base64
import play.api.mvc.{Headers, RequestHeader}
import play.api.{Configuration, Logger}

object SpnegoAuthenticator {
  private val negotiate = "Negotiate"
}

class SpnegoAuthenticator(principal: String, keytab: String, debug: Boolean, domain: Option[String], path: Option[String], tokens: Tokens, cookieName: String) {

  import SpnegoAuthenticator._

  private val subject = new Subject(false, Set(new KerberosPrincipal(principal)).asJava, Set.empty[AnyRef].asJava, Set.empty[AnyRef].asJava)
  private val kerberosConfiguration = new KerberosConfiguration(keytab, principal, debug)

  private val loginContext = new LoginContext("", subject, null, kerberosConfiguration)
  loginContext.login()

  private val gssManager = Subject.doAs(loginContext.getSubject, new PrivilegedAction[GSSManager] {
    override def run: GSSManager = GSSManager.getInstance
  })

  private def cookieToken(ctx: RequestHeader): Option[Either[AuthenticationMessageWithToken, Token]] = try {

    ctx.cookies.find(_.name == cookieName).map {
      cookie =>
        Logger.debug("cookie found")
        cookie
    }.flatMap {
      cookie =>
        Some(tokens.parse(cookie.value)).filter(!_.expired).map {
          token => Logger.debug("spnego token inside cookie not expired"); token
        }
    }.map(Right(_))
  } catch {
    case e: TokenParseException => {
      Logger.error(s"Token parse failed with error => ${e.message}")
      Some(Left(new AuthenticationMessageWithToken("Malformed token " + e.message, None))) // malformed token in cookie
    }
  }

  private def clientToken(ctx: RequestHeader): Option[Array[Byte]] = ctx.headers.headers.collectFirst {
    case ("Authorization", v) => v
  }.filter(_.startsWith(negotiate)).map { authHeader =>
    Logger.debug("authorization header found")
    new Base64(0).decode(authHeader.substring(negotiate.length).trim)
  }

  private def kerberosCore(clientToken: Array[Byte]): Either[AuthenticationMessageWithToken, Token] = {
    try {
      Logger.debug(s"Initiating login context with doAs user => ${loginContext.getSubject}")
      val (maybeServerToken, maybeToken) = Subject.doAs(loginContext.getSubject, new PrivilegedExceptionAction[(Option[Array[Byte]], Option[Token])] {
        override def run: (Option[Array[Byte]], Option[Token]) = {
          val gssContext = gssManager.createContext(null: GSSCredential)
          try {
            (
              Option(gssContext.acceptSecContext(clientToken, 0, clientToken.length)),
              if (gssContext.isEstablished) Some(tokens.create(gssContext.getSrcName.toString)) else None
            )
          } catch {
            case e: Throwable =>
              Logger.error(s"error in establishing security context ${e.toString}")
              throw e
          } finally {
            gssContext.dispose()
          }
        }
      })
      if (Logger.isDebugEnabled)
        Logger.debug(s"maybeServerToken ${maybeServerToken.isDefined} maybeToken ${maybeToken.isDefined}")
      maybeToken.map { token =>
        Logger.debug("Received new token")
        Right(token)
      }.getOrElse {
        Logger.debug("no token received but if there is a serverToken then negotiations are ongoing")
        Left(new AuthenticationMessageWithToken("No token received but if there is a serverToken then negotiations are ongoing",
          maybeServerToken.map(" " + new Base64(0).encodeToString(_))))
      }
    } catch {
      case e: PrivilegedActionException => e.getException match {
        case e: IOException => throw e // server error
        case e: Throwable =>
          Logger.error(s"Negotiation failed with exeption ${e.toString}")
          Left(new AuthenticationMessageWithToken(s"Negotiation failed with exception ${e.toString}", Some("")))
      }
    }
  }

  private def kerberosNegotiate(ctx: RequestHeader): Option[Either[AuthenticationMessageWithToken, Token]] = clientToken(ctx).map(kerberosCore)

  private def initiateNegotiations: Either[AuthenticationMessageWithToken, Token] = {
    Logger.debug("no negotiation header found, initiating negotiations")
    Left(new AuthenticationMessageWithToken("Kerberos Credentials missing", Some("")))
  }

  def apply(ctx: RequestHeader): Either[AuthenticationMessageWithToken, Token] = cookieToken(ctx).orElse(kerberosNegotiate(ctx)).getOrElse(initiateNegotiations)
}
