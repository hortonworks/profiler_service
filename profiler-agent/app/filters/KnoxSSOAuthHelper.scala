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

import java.io.ByteArrayInputStream
import java.io.UnsupportedEncodingException
import java.security.cert.CertificateException
import java.security.cert.CertificateFactory
import java.security.interfaces.RSAPublicKey
import java.util.Date

import com.nimbusds.jose.JWSObject
import com.nimbusds.jose.crypto.RSASSAVerifier
import com.nimbusds.jwt.SignedJWT

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import auth.KnoxSSOException
import play.api.Logger

class KnoxSSOAuthHelper {
  private val pemHeader = "-----BEGIN CERTIFICATE-----"
  private val pemFooter = "-----END CERTIFICATE-----"

  //Handle the cases where user specifies full PEM key with header and footer
  private def getFullPem(pem:String):String = {
    val prefixTrimmedPem = pem.stripPrefix(pemHeader).trim
    val suffixTrimmedPem = prefixTrimmedPem.stripSuffix(pemFooter).trim
    pemHeader + "\n" + suffixTrimmedPem + "\n" + pemFooter
  }

  def parseRSAPublicKey(pem:String, certificateFactory: CertificateFactory):Future[RSAPublicKey] = {
    val fullPem = getFullPem(pem)
    Logger.debug(s"Constructed full Knox pem => $fullPem")
    val publicKey = Try{
      val byteArrayInputStream = new ByteArrayInputStream(fullPem.getBytes("UTF8"))
      val cert = certificateFactory.generateCertificate(byteArrayInputStream)
      cert.getPublicKey
    }

    publicKey match {
      case Success(key) => {
        Future.successful(key.asInstanceOf[RSAPublicKey])
      }
      case Failure(e) => {
        e match {
          case ce:CertificateException => {
              Logger.error(s"CertificateException - PEM may be corrupt => ${ce.getMessage}")
              Future.failed(KnoxSSOException("CertificateException - PEM may be corrupt"))
          }

          case uee:UnsupportedEncodingException => {
            Logger.error(s"CertificateException - PEM may be corrupt => ${uee.getMessage}")
            Future.failed(KnoxSSOException("UnsupportedEncodingException =>  " + uee.getMessage))
          }
        }
      }
    }
  }

  def validateSignature(signedJWT: SignedJWT, publicKeyVerifier: RSASSAVerifier):Future[Boolean] = {
    val isValidationCompleted = Try{
      if(JWSObject.State.SIGNED == signedJWT.getState){
        Logger.info(s"SSO token is in signed state. Will attempt to validate it")
        signedJWT.verify(publicKeyVerifier)
      } else {
        Logger.error(s"SSO token is not in signed state. Failing SSO authentication")
        false
      }
    }

    isValidationCompleted match {
      case Success(validationResult) => {
        if(validationResult){
          Logger.info("Signature in SSO token in request is valid")
        }else{
          Logger.error("Signature in SSO token in request is invalid")
        }
        Future.successful(validationResult)
      }
      case Failure(e) => {
        Logger.error(s"SSO token signature validation failed with exception ${e.getMessage}")
        Future.failed(KnoxSSOException(s"Exception while validating the SSO token signature ${e.getMessage}"))
      }
    }
  }

  def validateTokenExpiry(signedJWT: SignedJWT):Future[Boolean] = {
      val isExpiryValidationCompleted = Try{
        val expiryTime = signedJWT.getJWTClaimsSet.getExpirationTime
        (expiryTime == null || new Date().before(expiryTime))
      }

    isExpiryValidationCompleted match {
        case Success(expiryValidationResult) => {
          if(expiryValidationResult){
            Logger.info("SSO token in request has not expired")
          }else{
            Logger.error("SSO token in request has expired")
          }
          Future.successful(expiryValidationResult)
        }
        case Failure(e) => {
          Logger.error(s"SSO token expiry validation failed with exception ${e.getMessage}")
          Future.failed(KnoxSSOException(s"Exception while validating the SSO token expiry ${e.getMessage}"))
        }
      }
  }

  def validateSSOToken(signedJWT: SignedJWT, publicKeyVerifier:RSASSAVerifier, ec: ExecutionContext): Future[Boolean] = {
    implicit val executionContext = ec
    validateSignature(signedJWT, publicKeyVerifier).flatMap(isSignatureValid =>
          validateTokenExpiry(signedJWT).flatMap(isNotExpired => {
            val isValidToken = isSignatureValid && isNotExpired
            Future.successful(isValidToken)
          })
    )
  }
}
