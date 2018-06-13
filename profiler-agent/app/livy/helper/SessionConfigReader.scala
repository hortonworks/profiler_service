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

package livy.helper

import livy.session.models.LivyRequests.CreateSession
import play.api.Configuration
import play.api.libs.json.Json

object SessionConfigReader {

  def readSessionConfig(configuration: Configuration, group: String): CreateSession = {
    val proxyUser = configuration.getString("profiler.user").getOrElse("dpprofiler")
    val groupPrefix = s"livy.session.config.$group"
    val name = configuration.getString(s"$groupPrefix.name")
      .getOrElse("profiler_session")
    val heartbeatTimeoutInSecond: Option[Int] = Some(configuration.getInt(s"$groupPrefix.heartbeatTimeoutInSecond")
      .getOrElse(172800))
    val driverMemory = configuration.getString(s"$groupPrefix.driverMemory")
    val driverCores = configuration.getInt(s"$groupPrefix.driverCores")
    val executorMemory = configuration.getString(s"$groupPrefix.executorMemory")
    val executorCores = configuration.getInt(s"$groupPrefix.executorCores")
    val numExecutors = configuration.getInt(s"$groupPrefix.numExecutors")
    val queue = configuration.getString(s"$groupPrefix.queue")
    val conf = configuration.getString(s"$groupPrefix.conf").
      map(Json.toJson(_).as[Map[String, String]])
    CreateSession("spark", proxyUser, name, heartbeatTimeoutInSecond, driverMemory,
      driverCores, executorMemory, executorCores, numExecutors, queue, conf)
  }

}
