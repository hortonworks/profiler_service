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

package job.runner

import domain.JobStatus.JobStatus
import domain._
import job.WSResponseMapper
import org.joda.time.DateTime
import play.api.Logger
import play.api.http.Status
import play.api.libs.json.{JsObject, Json}
import play.api.libs.ws.{WSAuthScheme, WSClient, WSRequest}

import scala.concurrent.Future

class LivyJobRunner(livyConf: Map[String, String], ws: WSClient, jobDataBuilder: JobDataBuilder, secured: Boolean) extends JobRunner with WSResponseMapper {

  import domain.JsonFormatters._

  import scala.concurrent.ExecutionContext.Implicits.global

  Logger.info(s"Livy Spenego : $secured")

  private val livyurl = livyConf.get("url").get + "/batches"

  def maskedJsObj(jsObject: JsObject) = {
    var argString = (jsObject \ "args") (0).as[String]
    val argsData = Json.parse(argString).as[JsObject]
    val clusterConfigs = (argsData \ "clusterconfigs").as[JsObject] ++ Json.obj("atlasPassword" -> "*******")
    val args = argsData ++ Json.obj("clusterconfigs" -> clusterConfigs)
    jsObject ++ Json.obj("args" -> Seq(args.toString()))
  }

  override def start(profiler: ProfilerAndProfilerInstance, jobTask: JobTask): Future[ProfilerJob] = {
    val config = profiler.profilerConf
    val data = jobDataBuilder.getJson(profiler, jobTask)
    val jsObject = Json.obj("queue" -> profiler.profilerInstance.queue) ++
      config ++ Json.obj("args" -> Json.toJson(Seq(data.toString())))
    submitJobToLivy(profiler, jsObject, jobTask)
  }

  private def submitJobToLivy(profiler: ProfilerAndProfilerInstance, body: JsObject, jobTask: JobTask) = {
    val maskedBody = maskedJsObj(body)
    Logger.info(maskedBody.toString())
    val respFuture =
      withAuth(ws.url(livyurl))
        .withHeaders("Content-Type" -> "application/json", "X-Requested-By" -> "profiler")
        .post(body)

    respFuture.flatMap {
      implicit response =>
        parseResponse({
          ProfilerJob(None, profiler.profilerInstance.id.get, profiler.profilerInstance.name, JobStatus.STARTED, profiler.profiler.jobType, Json.toJson(jobTask).as[JsObject],
            response.json.as[JsObject], "Livy Job", "", (body \ "proxyUser").asOpt[String].getOrElse(""), jobTask.submitter, DateTime.now(), DateTime.now())
        }, Status.CREATED)
    }
  }

  private def getJobStatus(status: String): JobStatus = {
    status match {
      case "success" => JobStatus.SUCCESS
      case "error" | "dead" => JobStatus.FAILED
      case _ => JobStatus.RUNNING
    }
  }

  private def withAuth(wSRequest: WSRequest): WSRequest = {
    if (secured) wSRequest.withAuth("", "", WSAuthScheme.SPNEGO)
    else wSRequest
  }

  override def status(profilerJob: ProfilerJob): Future[ProfilerJob] = {
    val jobId = (profilerJob.details \ "id").as[Long].toString
    val url = s"$livyurl/${jobId}"
    val respFuture =
      withAuth(ws.url(url))
        .withHeaders("Content-Type" -> "application/json", "X-Requested-By" -> "profiler")
        .get()

    respFuture.flatMap {
      implicit response =>
        parseResponse {
          val jobDetails = profilerJob.details.deepMerge(response.json.as[JsObject])
          val jobStatus = getJobStatus((jobDetails \ "state").as[String])
          // Haven't found a way of getting the right end time for a given job.
          // Hence setting this to None until then
          val endTime = None

          profilerJob.copy(details = jobDetails, end = endTime, status = jobStatus, lastUpdated = DateTime.now())
        }
    }
  }

  override def kill(profilerJob: ProfilerJob): Future[ProfilerJob] = {
    val jobId = (profilerJob.details \ "id").as[String]
    val respFuture =
      withAuth(ws.url(s"$livyurl/${jobId}"))
        .withHeaders("Content-Type" -> "application/json", "X-Requested-By" -> "profiler")
        .delete()

    respFuture.flatMap {
      implicit response =>
        parseResponse {
          profilerJob.copy(status = JobStatus.KILLED)
        }
    }
  }
}
