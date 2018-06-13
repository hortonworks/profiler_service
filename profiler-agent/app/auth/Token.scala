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


import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest

import play.api.Logger
import scala.util.{Success, Try}
import org.apache.commons.codec.binary.Base64

case class TokenParseException(message: String) extends Exception(message)

class Tokens(tokenValidity: Long, signatureSecret: Array[Byte]) {
  private def newExpiration: Long = System.currentTimeMillis + tokenValidity

  private[auth] def sign(token: Token): String = {
    val md = MessageDigest.getInstance("SHA")
    md.update(token.principal.getBytes(UTF_8))
    val bb = ByteBuffer.allocate(8)
    bb.putLong(token.expiration)
    md.update(bb.array)
    md.update(signatureSecret)
    new Base64(0).encodeToString(md.digest)
  }

  def create(principal: String): Token = Token(principal, newExpiration)

  def parse(tokenString: String): Token = tokenString.split("&") match {
    case Array(principal, expirationString, signature) => Try(expirationString.toLong) match {
      case Success(expiration) => {
        val token = Token(principal, expiration)
        if (sign(token) != signature) throw TokenParseException("incorrect signature")
        token
      }
      case _ => throw TokenParseException("expiration not a long")
    }
    case _ => throw TokenParseException("incorrect number of fields")
  }

  def serialize(token: Token): String = List(token.principal, token.expiration, sign(token)).mkString("&")
}

case class Token private[auth] (principal: String, expiration: Long) {
  def expired: Boolean = System.currentTimeMillis > expiration
}
