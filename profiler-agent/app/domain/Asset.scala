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

import java.time.LocalDateTime

import com.hortonworks.dataplane.profilers.commons.domain.AssetType.AssetType
import domain.JobStatus.JobStatus
import org.joda.time.DateTime
import play.api.libs.json.JsObject

case class Asset(id: String, assetType: AssetType, data: JsObject, description: Option[String] = None)

case class AssetJobHistory(id: Option[Long], profilerInstanceName: String, assetId: String, jobId: Long, status: JobStatus, lastUpdated: DateTime = DateTime.now())

case class OperatedAssetCountsPerDay(day: String, assetsCount: Map[String, Int])

case class ProfilerRunInfo(profilerInfo: ProfilerInstance, assetInfo: Option[AssetJobHistory])


object AssetJobHistory {
  def getAssetsHistory(assets: Seq[Asset], job: ProfilerJob) = {
    assets.map {
      asset =>
        AssetJobHistory(None, job.profilerInstanceName, asset.id, job.id.get, job.status)
    }
  }
}

/**
  * Wrapper case class to hold the asset and its corresponding priority. priority and createdAt will be used to
  * prioritize the asset while submission.
  * @param asset Asset to be submitted
  * @param priority Priority defined by the asset selector
  * @param createdAt Time at which the Asset is submitted by the selector
  */
case class AssetEnvelop(asset: Asset, priority: Int, createdAt: LocalDateTime)

case class DatasetAssetId(id: Option[Long],datasetName: String, assetId: String, priority: Option[Long])

case class DatasetAndAssetIds(datasetName: String, assetIds: Seq[String])

case class TableResult( database : String, table : String, column : String, label: String, percentMatch : Double = 100, status: String)

case class ClassificationAttribute(status : String)

case class Classification(typeName: String, attributes: ClassificationAttribute)

case class ClassificationDefinition (postData: Option[Seq[Classification]], putData: Option[Seq[Classification]])

case class Column(name: String, guid: String, classifications: ClassificationDefinition)

case class AssetTagInfo(databaseName: String, tableName: String, columns: Seq[Column])