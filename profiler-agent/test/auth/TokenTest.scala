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

import org.scalatest.WordSpecLike
import org.scalatest.mockito.MockitoSugar
import org.scalatest.Matchers._
import org.mockito.Mockito._
import java.nio.charset.StandardCharsets.UTF_8

class TokenTest extends WordSpecLike with MockitoSugar{
  val signatureSecret = "secret"
  val principal = "sample"
  val tokenSigner = new Tokens(1000000L, signatureSecret.getBytes(UTF_8))
  val token = tokenSigner.create(principal)

  "Token validation class " must {
    " be able to generate a token from given principal " in {
      val constructedToken = tokenSigner.create(principal)
      assert(constructedToken.isInstanceOf[Token])
    }

    " be able to serialize a valid Token object " in {
      val serializedToken = tokenSigner.serialize(token)
      val parts = serializedToken.split("&")
      assert(parts.length == 3)
    }

    " be able to parse a valid serialized token " in {
        val serializedToken = tokenSigner.serialize(token)
        val parsedToken = tokenSigner.parse(serializedToken)
        assert(parsedToken.isInstanceOf[Token])
    }

    " throw exception while parsing an invalid serialized token " in {
      the [TokenParseException] thrownBy tokenSigner.parse("somegarbage") should have
        message("incorrect number of fields")

      the [TokenParseException] thrownBy tokenSigner.parse("ad&sdkfnd&ksdnf") should have
        message("expiration not a long")

      val generatedToken = tokenSigner.serialize(token)
      val corruptedToken = generatedToken.substring(0, generatedToken.length - 1)
      the [TokenParseException] thrownBy tokenSigner.parse(corruptedToken) should have
        message("incorrect signature")
    }

    " be able to validate token expiry time " in {
      val tokenSignerQuickExpiry = new Tokens(100L, signatureSecret.getBytes(UTF_8))
      val token = tokenSignerQuickExpiry.create(principal)
      Thread.sleep(200L)
      assert(token.expired == true)
    }
  }
}
