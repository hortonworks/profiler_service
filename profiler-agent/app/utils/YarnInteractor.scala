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
package utils

import javax.inject.{Inject, Singleton}

import controllers.JsonApi
import play.api.libs.ws.{WSAuthScheme, WSClient, WSRequest}
import play.api.mvc.{Action, Controller}
import play.api.{Configuration, Logger}
import domain.YarnQueueEntry

import scala.concurrent.{ExecutionContext, Future}

@Singleton
class YarnInteractor @Inject()(configuration: Configuration, ws: WSClient)(implicit exec: ExecutionContext)
  extends Controller with JsonApi{
  private val yarnurl = configuration.getString("yarn.rm.url").get
  private val secured = configuration.getBoolean("dpprofiler.secured").getOrElse(false)

  import scala.concurrent.ExecutionContext.Implicits.global
  import domain.JsonFormatters._

  def getYarnQueueSet():Future[Set[String]] = {
    val apiEndpoint = s"http://$yarnurl/ws/v1/cluster/scheduler"
    Logger.info(s"Got YARN RM URL => $yarnurl")
    withAuth(ws.url(apiEndpoint))
      .withHeaders("Content-Type" -> "application/json", "X-Requested-By" -> "profiler")
      .get() flatMap (
      response => {
        response.status match {
          case 200 => {
            val rootQueue = (response.json \ "scheduler" \ "schedulerInfo").as[YarnQueueEntry]
            Future.successful(getQueueNames(rootQueue))
          }
          case _ =>
            Future.failed(new Exception(s"Failed to get response from Yarn. Response code ${response.status.toString} "
              + s". " + s"Error message  ${response.json.toString}"))

        }
      }
      )
  }

  private def getQueueNames(queueEntry: YarnQueueEntry):Set[String] = {
    if(queueEntry.`type`.isDefined && !queueEntry.`type`.get.equals("capacityScheduler")){
      Set(queueEntry.queueName)
    }else{
      val childQueues = (queueEntry.queues.get \ "queue").as[List[YarnQueueEntry]]
      childQueues.flatMap(getQueueNames).toSet
    }
  }

  private def withAuth(wSRequest: WSRequest): WSRequest = {
    if (secured) wSRequest.withAuth("", "", WSAuthScheme.SPNEGO)
    else wSRequest
  }
}
