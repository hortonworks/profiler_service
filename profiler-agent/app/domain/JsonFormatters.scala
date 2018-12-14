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

import play.api.libs.json.{Format, Json, Reads}
import profilers.dryrun.models.DryRunSchemaForProfiler

object JsonFormatters {

  val defaultJson = Json.using[Json.WithDefaultValues]

  implicit val jobTypeReads = Reads.enumNameReads(JobType)
  implicit val jobStatusReads = Reads.enumNameReads(JobStatus)
  implicit val aggTypeReads = Reads.enumNameReads(AggregationType)

  import profilers.metadata.parsers.MetaDataManagementSchemaParsers._
  implicit val dryRunSchemaForProfilerFormat: Format[DryRunSchemaForProfiler] =
    Json.format[DryRunSchemaForProfiler]

  implicit val profilerReads = defaultJson.reads[Profiler]
  implicit val profilerWrites = Json.writes[Profiler]

  implicit val profilerInstanceReads = defaultJson.reads[ProfilerInstance]
  implicit val profilerInstanceWrites = Json.writes[ProfilerInstance]

  implicit val assetReads = defaultJson.reads[Asset]
  implicit val assetWrites = Json.writes[Asset]

  implicit val jobTaskReads = defaultJson.reads[JobTask]
  implicit val jobTaskWrites = Json.writes[JobTask]

  implicit val profilerJobReads = defaultJson.reads[ProfilerJob]
  implicit val profileJobrWrites = Json.writes[ProfilerJob]

  implicit val errorWrites = Json.writes[Error]
  implicit val errorsWrites = Json.writes[Errors]

  implicit val jobHistoryReads = defaultJson.reads[AssetJobHistory]
  implicit val jobHistoryWrites = Json.writes[AssetJobHistory]

  implicit val auditMetricReads = Json.using[Json.WithDefaultValues].reads[Metric]
  implicit val auditMetricWrites = Json.writes[Metric]

  implicit val auditQueryReads = defaultJson.reads[InteractiveQuery]
  implicit val auditQueryWrites = Json.writes[InteractiveQuery]

  implicit val cacheMetricsReads = defaultJson.reads[CacheMetrics]
  implicit val cacheMetricsWrites = Json.writes[CacheMetrics]

  implicit val profilerAndProfilerInstanceReads = defaultJson.reads[ProfilerAndProfilerInstance]
  implicit val profilerAndProfilerInstanceWrites = Json.writes[ProfilerAndProfilerInstance]

  implicit val assetSourceDefinitionReads = defaultJson.reads[AssetSourceDefinition]
  implicit val assetSourceDefinitionWrites = Json.writes[AssetSourceDefinition]

  implicit val assetFilterRead = defaultJson.reads[AssetFilterDefinition]
  implicit val assetFilterWrite = Json.writes[AssetFilterDefinition]

  implicit val assetSelectorDefinitionReads = defaultJson.reads[AssetSelectorDefinition]
  implicit val assetSelectorDefinitionWrites = Json.writes[AssetSelectorDefinition]

  implicit val datasetAssetIdReads = defaultJson.reads[DatasetAssetId]
  implicit val datasetAssetIdWrites = Json.writes[DatasetAssetId]

  implicit val datasetAndAssetIdsReads = defaultJson.reads[DatasetAndAssetIds]
  implicit val datasetAndAssetIdsWrites = Json.writes[DatasetAndAssetIds]

  implicit val profilerJobInfoRefinedReads = defaultJson.reads[ProfilerInfoRefined]
  implicit val profilerJobInfoRefinedWrites = Json.writes[ProfilerInfoRefined]

  implicit val profilerJobsCountReads = defaultJson.reads[ProfilerJobsCount]
  implicit val profilerJobsCountWrites = Json.writes[ProfilerJobsCount]

  implicit val filteredProfilerJobReads = defaultJson.reads[FilteredProfilerJob]
  implicit val filteredProfilerJobWrites = Json.writes[FilteredProfilerJob]

  implicit val profilerAssetsCountReads = defaultJson.reads[ProfilerAssetsCount]
  implicit val profilerAssetsCountWrites = Json.writes[ProfilerAssetsCount]

  implicit val operatedAssetCountsPerDayReads = defaultJson.reads[OperatedAssetCountsPerDay]
  implicit val operatedAssetCountsPerDayWrites = Json.writes[OperatedAssetCountsPerDay]

  implicit val profilerRunInfoReads = defaultJson.reads[ProfilerRunInfo]
  implicit val profilerRunInfoWrites = Json.writes[ProfilerRunInfo]

  implicit val tableResultReads = defaultJson.reads[TableResult]
  implicit val tableResultWrites = Json.writes[TableResult]

  implicit val classificationAttributeReads = defaultJson.reads[ClassificationAttribute]
  implicit val classificationAttributeWrites = Json.writes[ClassificationAttribute]

  implicit val classificationReads = defaultJson.reads[Classification]
  implicit val classificationWrites = Json.writes[Classification]

  implicit val classificationDefinitionReads = defaultJson.reads[ClassificationDefinition]
  implicit val classificationDefinitionWrites = Json.writes[ClassificationDefinition]

  implicit val columnReads = defaultJson.reads[Column]
  implicit val columnWrites = Json.writes[Column]

  implicit val assetTagInfoReads = defaultJson.reads[AssetTagInfo]
  implicit val assetTagInfoWrites = Json.writes[AssetTagInfo]

  implicit val changeNotificationLogReads = defaultJson.reads[ChangeNotificationLog]
  implicit val changeNotificationLogWrites = Json.writes[ChangeNotificationLog]
  
  implicit val queueEntryReads = defaultJson.reads[YarnQueueEntry]
  implicit val queueEntryWrites = defaultJson.writes[YarnQueueEntry]

  implicit val selectorConfigReads = defaultJson.reads[SelectorConfig]
  implicit val selectorConfigWrites = defaultJson.writes[SelectorConfig]

  implicit val profilerInstanceAndSelectorConfigReads = defaultJson.reads[ProfilerInstanceAndSelectorConfig]
  implicit val profilerInstanceAndSelectorConfigWrites = defaultJson.writes[ProfilerInstanceAndSelectorConfig]

  implicit val profilerInstanceAndSelectorDefinitionReads = defaultJson.reads[ProfilerInstanceAndSelectorDefinition]
  implicit val profilerInstanceAndSelectorDefinitionWrites = defaultJson.writes[ProfilerInstanceAndSelectorDefinition]
}
