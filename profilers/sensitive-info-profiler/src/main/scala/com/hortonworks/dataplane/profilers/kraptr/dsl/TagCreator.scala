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

package com.hortonworks.dataplane.profilers.kraptr.dsl

import com.hortonworks.dataplane.profilers.kraptr.behaviour.Decider
import com.hortonworks.dataplane.profilers.kraptr.models.dsl.MatchType.MatchType
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.util.Try

@SerialVersionUID(847600948789722L)
class TagCreator(val decider: Decider, val tags: Seq[String], val matchType: MatchType, val confidence: Double) extends Serializable {


  override def toString: String = {
    s"TagCreator($tags,$matchType,$confidence)"
  }

  @transient private lazy val logger = Logger(LoggerFactory.getLogger(getClass.getName))

  def apply(token: String): Seq[String] = {
    Try(decider.process(token)).map(
      if (_) tags else List.empty
    ) recover {
      case exception => logger.warn(s"Decider ${decider.getClass.getSimpleName} failed" +
        s" to process token $token", exception)
        List.empty
    } getOrElse List.empty
  }
}



