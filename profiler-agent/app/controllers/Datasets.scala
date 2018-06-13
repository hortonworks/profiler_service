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

import domain._
import javax.inject.{Inject, Singleton}
import job.interactive.{LivyInteractiveRunner, LivyQueryCache}
import play.api.Logger
import play.api.libs.json.Json
import play.api.mvc.{Action, Controller}
import repo.DatasetRepo

import scala.concurrent.{ExecutionContext, Future}

@Singleton
class Datasets @Inject()(datasetRepo: DatasetRepo, livyInteractiveRunner: LivyInteractiveRunner
                         , livyQueryCache: LivyQueryCache)(implicit exec: ExecutionContext)
  extends Controller with JsonApi {

  import domain.JsonFormatters._

  def add = Action.async(parse.json) { req =>

    Logger.info("Datasets Controller: Receives add DatasetAndAssetIds request")

    req.body
      .validate[DatasetAndAssetIds]
      .map { dsAndAssetIds =>

        val dsName = dsAndAssetIds.datasetName
        val dsAssetSeq = dsAndAssetIds.assetIds.map { dsAsset =>
          DatasetAssetId(None, dsName, dsAsset, Some(10000))
        }

        val eventualDataSetMappingSave = livyInteractiveRunner.saveDatasetMapping(dsAndAssetIds)
          .flatMap {
            value =>
              datasetRepo.save(dsAssetSeq).map(res => Ok(
                Json.obj(
                  "numberOfRowsInserted" -> res,
                  "assetCount" -> value
                )
              ))
          }
        eventualDataSetMappingSave onSuccess {
          case _ => livyQueryCache.clearCache() onFailure {
            case error => Logger.error("Failed to refresh cached queries", error)
          }
        }
        eventualDataSetMappingSave
          .recover(mapErrorWithLog(e => Logger.error(s"Datasets Controller: Adding DatasetAndAssetIds $dsAndAssetIds failed with message ${e.getMessage}", e)))

      }
      .getOrElse {
        Logger.warn("Datasets Controller: Failed to map request to DatasetAndAssetIds entity")
        Future.successful(BadRequest)
      }
  }

  def saveOrUpdate = Action.async(parse.json) { req =>

    Logger.info("Datasets Controller: Receives saveOrUpdate request")

    req.body
      .validate[DatasetAndAssetIds]
      .map { dsAndAssetIds =>

        val eventualDataSetMappingSave = livyInteractiveRunner.saveDatasetMapping(dsAndAssetIds)
          .flatMap {
            value =>
              datasetRepo.saveOrUpdate(dsAndAssetIds)
                .map(res => Ok(
                  Json.obj(
                    "numberOfRowsDeleted" -> res._1,
                    "numOfRowsInserted" -> res._2,
                    "assetCount" -> value
                  )
                ))
          }
        eventualDataSetMappingSave onSuccess {
          case _ => livyQueryCache.clearCache()
        }
        eventualDataSetMappingSave.recover(mapErrorWithLog(e => Logger.error(s"Datasets Controller: Saving/Updating DatasetAndAssetIds $dsAndAssetIds failed with message ${e.getMessage}", e)))

      }
      .getOrElse {
        Logger.warn("Datasets Controller: Failed to map request to DatasetAndAssetIds entity")
        Future.successful(BadRequest)
      }
  }

  def saveTagsOnHDFS = Action.async(parse.json) { req =>

    req.body
      .validate[AssetTagInfo]
      .map { assetTagInfo =>

        val tableResults: Seq[TableResult] = assetTagInfo.columns.flatMap { col =>

          val putData = col.classifications.putData.getOrElse(Nil)
          val postData = col.classifications.postData.getOrElse(Nil)


          (putData ++ postData).map { classification =>
            TableResult(assetTagInfo.databaseName, assetTagInfo.tableName, col.name, classification.typeName, status = classification.attributes.status)
          }
        }

        if (tableResults.isEmpty) {

          Logger.info(s"No data found for table ${assetTagInfo.tableName} : Skipping this")
          Future.successful(Ok(Json.obj("message" -> "no data found")))

        } else {

          val eventualSaveTagsOnHdfs = livyInteractiveRunner.saveTags(tableResults, assetTagInfo.databaseName, assetTagInfo.tableName).map { res =>
            Ok(Json.toJson(res))
          }
          eventualSaveTagsOnHdfs onSuccess {
            case _ => livyQueryCache.clearCache() onFailure {
              case error => Logger.error("Failed to refresh cached queries", error)
            }
          }
          eventualSaveTagsOnHdfs
            .recover(mapErrorWithLog(e => Logger.error(s"Datasets Controller: saveTagsOnHDFS with assetTagInfo $assetTagInfo failed with message ${e.getMessage}", e)))

        }
      }
      .getOrElse {
        Logger.warn("Datasets Controller: Failed to map request to AssetTagInfo entity")
        Future.successful(BadRequest)
      }
  }

  def deleteByDatasetName(dsName: String) = Action.async {

    Logger.info("Datasets Controller: Received deleteByDatasetName request")

    datasetRepo.deleteByDatasetName(dsName).map { res =>
      Ok(Json.obj("totalRowsDeleted" -> res))
    }
      .recover(mapErrorWithLog(e => Logger.error(s"Datasets Controller: deleteByDatasetName with dataset name $dsName failed with message ${e.getMessage}", e)))
  }

  def getExistingProfiledAssetCount(profilerName: String) = Action.async {

    Logger.debug("Datasets Controller: Received getExistingProfiledAssetCount request")

    livyInteractiveRunner.getExistingProfiledAssetCount(profilerName).map { res =>
      Ok(Json.obj("assetCount" -> res))
    }
      .recover(mapErrorWithLog(e => Logger.error(s"Datasets Controller: getExistingProfiledAssetCount with profiler name $profilerName failed ")))

  }

  def getByDsName(dsName: String) = Action.async {

    Logger.info("Datasets Controller: Received getByDsName request")

    datasetRepo.getByDsName(dsName).map { dsassetSeq =>
      val assetIds = dsassetSeq.map { dsasset =>
        dsasset.assetId
      }
      Ok(Json.toJson(DatasetAndAssetIds(dsName, assetIds)))
    }
      .recover(mapErrorWithLog(e => Logger.error(s"Datasets Controller: getByDsName with dataset name $dsName failed with message ${e.getMessage}", e)))
  }

  def getAll = Action.async {

    datasetRepo.getAll.map { res =>
      Ok(Json.toJson(res))
    }
      .recover(mapErrorWithLog(e => Logger.error(s"Datasets Controller: getAll failed with message ${e.getMessage}", e)))
  }

  def getProfilerDatasetAssetCount(dsName: String, profilerinstancename: String, startTime: Long, endTime: Long) = Action.async {

    Logger.info("Datasets Controller: Received getSensitiveProfDatasetAssetCount request")

    datasetRepo.profilerDatasetAssetCount(profilerinstancename, dsName, startTime, endTime).map { dsasset =>

      val assetCountVal = dsasset.map(_._2).getOrElse(0)
      Ok(Json.obj("datasetName" -> dsName, "assetCount" -> assetCountVal))

    }
      .recover(mapErrorWithLog(e => Logger.error(s"Datasets Controller: getSensitiveProfDatasetAssetCount with dataset name $dsName and profilerinstancename $profilerinstancename failed with message ${e.getMessage}", e)))
  }

}
