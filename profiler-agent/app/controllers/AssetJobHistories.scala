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

package controllers

import javax.inject.{Inject, Singleton}

import domain.JobStatus.JobStatus
import org.joda.time.DateTime
import play.api.Logger
import play.api.libs.json.Json
import play.api.mvc.{Action, Controller}
import repo.AssetJobHistoryRepo

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}

@Singleton
class AssetJobHistories @Inject()(assetJobHistoryRepo: AssetJobHistoryRepo)(implicit exec: ExecutionContext)
  extends Controller with JsonApi {

  import domain.JsonFormatters._

  def getOperatedAssetsCountPerDay(profilerName: String, startTime:Long, endTime:Long) = Action.async {

    Logger.info("AssetJobHistories Controller: Received getOperatedAssetsCountForWeekDays request")

    assetJobHistoryRepo.getOperatedAssetsCountPerDay(profilerName, startTime, endTime).map{ results =>

      Ok(Json.toJson(results))

    }
      .recover(mapErrorWithLog(e => Logger.error(s"AssetJobHistories Controller: getOperatedAssetsCountForWeekDays with profilername  $profilerName  failed with message ${e.getMessage}", e)))
  }

  def getAll = Action.async {

    assetJobHistoryRepo.getAll.map{ res =>
      Ok(Json.toJson(res))
    }
      .recover(mapErrorWithLog(e => Logger.error(s"AssetJobHistories Controller: getAll failed with message ${e.getMessage}", e)))
  }

  def getProfilersLastRunInfoOnAsset(assetId: String) =  Action.async {

    assetJobHistoryRepo.getProfilersLastRunInfoOnAsset(assetId).map { res =>

      Ok(Json.toJson(res))

    }
      .recover(mapErrorWithLog(e => Logger.error(s"AssetJobHistories Controller: getProfilersLastRunInfoOnAsset with assetId $assetId failed with message ${e.getMessage}", e)))
  }
}
