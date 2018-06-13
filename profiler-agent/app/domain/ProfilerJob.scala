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

import domain.JobStatus.JobStatus
import domain.JobType.JobType
import org.joda.time.DateTime
import play.api.libs.json.JsObject


case class ProfilerJob(
                        id: Option[Long],
                        profilerInstanceId: Long, profilerInstanceName: String,
                        status: JobStatus, jobType: JobType,
                        conf: JsObject, details: JsObject,
                        description: String, jobUser: String,
                        queue: String, submitter: String = "unknown",
                        start: DateTime , lastUpdated: DateTime,
                        end: Option[DateTime] = None
                      )

case class FilteredProfilerJob(
                        id: Option[Long],
                        profilerId: Long,
                        profiler: String,
                        status: JobStatus,
                        queue: String,
                        sparkUiUrl: Option[String],
                        driverLogUrl: Option[String],
                        start: DateTime,
                        end: Option[DateTime]
                      )

case class ProfilerInfoRefined(id: Option[Long], name: String, displayName: String,
                               active: Boolean = true, version: Long = 1)

case class ProfilerAssetsCount(profilerInfo: ProfilerInfoRefined,
                               assetsCount: Long)

case class ProfilerJobsCount(profilerInfo: ProfilerInfoRefined,
                             jobsCount: Map[String, Int])

object ProfilerJob {
  def isCompleted(jobStatus: JobStatus) =
    jobStatus == JobStatus.FAILED ||
      jobStatus == JobStatus.SUCCESS ||
      jobStatus == JobStatus.KILLED
}

object JobStatus extends Enumeration {
  type JobStatus = Value
  val STARTED, RUNNING, SUCCESS, FAILED, KILLED, UNKNOWN = Value
  def defaultMap = Map(JobStatus.UNKNOWN.toString -> 0, JobStatus.SUCCESS.toString -> 0,JobStatus.STARTED.toString -> 0,JobStatus.RUNNING.toString -> 0,JobStatus.FAILED.toString -> 0,JobStatus.KILLED.toString -> 0)
}

case class JobTask(profilerInstanceName: String, assets: Seq[Asset] = Nil, submitter: String = "default")
