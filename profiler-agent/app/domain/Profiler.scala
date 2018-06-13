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

package domain

import com.hortonworks.dataplane.profilers.commons.domain.AssetType.AssetType
import domain.JobType.JobType
import org.joda.time.DateTime
import play.api.libs.json.JsObject

/**
  *
  * @param profilerConf Configuration for profiler. Eg jar, files, memory
  */
case class Profiler(val id: Option[Long], name: String,
                    version: String = "1.0", jobType: JobType, assetType: AssetType,
                    profilerConf: JsObject, user: String, description: String, created: Option[DateTime])

/**
  *
  * @param profilerConf Configuration for profiler. Eg jar, files, memory. (This conf will get priority over profiler)
  * @param jobConf      Configuration to job. It will be passed to running job
  */
case class ProfilerInstance(id: Option[Long], name: String, displayName: String,
                            profilerId: Long, version: Long = 1,
                            profilerConf: JsObject, jobConf: JsObject,
                            active: Boolean = true, owner: String = "unknown", queue: String = "default",
                            description: String, created: Option[DateTime])

case class ProfilerAndProfilerInstance(profiler: Profiler, profilerInstance: ProfilerInstance) {

  def profilerConf = profiler.profilerConf.deepMerge(profilerInstance.profilerConf)

  def jobConf = profilerInstance.jobConf
}

object JobType extends Enumeration {
  type JobType = Value
  val Livy = Value
}