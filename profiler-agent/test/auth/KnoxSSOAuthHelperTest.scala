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

import java.security.cert.CertificateFactory
import java.security.interfaces.RSAPublicKey
import java.util.Date

import org.mockito.Mockito._
import com.nimbusds.jose.JWSObject
import filters.KnoxSSOAuthHelper
import org.scalatest.{BeforeAndAfterAll, WordSpecLike}
import org.scalatest.mockito.MockitoSugar
import play.api.test.{DefaultAwaitTimeout, FutureAwaits}
import com.nimbusds.jose.crypto.RSASSAVerifier
import com.nimbusds.jwt.{ReadOnlyJWTClaimsSet, SignedJWT}


class KnoxSSOAuthHelperTest extends WordSpecLike with MockitoSugar with FutureAwaits
  with DefaultAwaitTimeout with BeforeAndAfterAll {
  val certificateFactory = CertificateFactory.getInstance("X.509")

  "KnoxSSOAuthHelper " must {
    " return failure while parsing an invalid RSA key " in {
      val knosSSOHelper = new KnoxSSOAuthHelper()
      val invalidPemAsString = "someradomvalue"
      val caughtException = intercept[KnoxSSOException]{
        val parseResult = knosSSOHelper.parseRSAPublicKey(invalidPemAsString, certificateFactory)
        await(parseResult)
      }
      assert(caughtException.msg == "CertificateException - PEM may be corrupt")
    }

    " return an instance of RSAPublicKey after parsing a valid RSA key " in {
      val knosSSOHelper = new KnoxSSOAuthHelper()
      val keyResult = knosSSOHelper.parseRSAPublicKey(Utils.getValidRSAPublicKey().get, certificateFactory)
      await(keyResult)
      assert(keyResult.value.get.get.isInstanceOf[RSAPublicKey])
    }

    " return an instance of RSAPublicKey after parsing a valid RSA key with header and footer " in {
      val knosSSOHelper = new KnoxSSOAuthHelper()
      val keyResult = knosSSOHelper.parseRSAPublicKey(Utils.getValidRSAPublicKeyWithHeaderAndFooter().get, certificateFactory)
      await(keyResult)
      assert(keyResult.value.get.get.isInstanceOf[RSAPublicKey])
    }

    " return failure when SSO token is not signed " in {
      val knoxSSOHelper = new KnoxSSOAuthHelper()
      val signedJWTMock = mock[SignedJWT]
      val rsaKeyVerifier = mock[RSASSAVerifier]
      when(signedJWTMock.getState).thenReturn(JWSObject.State.UNSIGNED)
      val authResult = knoxSSOHelper.validateSignature(signedJWTMock, rsaKeyVerifier)
      await(authResult)
      assert(!authResult.value.get.get)
    }

    " return success when signature validation is successful " in {
      val knoxSSOHelper = new KnoxSSOAuthHelper()
      val signedJWTMock = mock[SignedJWT]
      val rsaKeyVerifier = mock[RSASSAVerifier]
      when(signedJWTMock.getState).thenReturn(JWSObject.State.SIGNED)
      when(signedJWTMock.verify(rsaKeyVerifier)).thenReturn(true)
      val authResult = knoxSSOHelper.validateSignature(signedJWTMock, rsaKeyVerifier)
      await(authResult)
      assert(authResult.value.get.get)
    }

    " return failure when token has expired " in {
      val knoxSSOHelper = new KnoxSSOAuthHelper()
      val signedJWTMock = mock[SignedJWT]
      val jwtClaimsSetMock = mock[ReadOnlyJWTClaimsSet]
      when(signedJWTMock.getJWTClaimsSet).thenReturn(jwtClaimsSetMock)
      when(jwtClaimsSetMock.getExpirationTime).thenReturn(new Date(0))
      val authResult = knoxSSOHelper.validateTokenExpiry(signedJWTMock)
      await(authResult)
      assert(!authResult.value.get.get)
    }

    " return success when token has not expired " in {
      val knoxSSOHelper = new KnoxSSOAuthHelper()
      val signedJWTMock = mock[SignedJWT]
      val jwtClaimsSetMock = mock[ReadOnlyJWTClaimsSet]
      when(signedJWTMock.getJWTClaimsSet).thenReturn(jwtClaimsSetMock)
      when(jwtClaimsSetMock.getExpirationTime).thenReturn(null)
      val authResult = knoxSSOHelper.validateTokenExpiry(signedJWTMock)
      await(authResult)
      assert(authResult.value.get.get)
    }

  }

}
