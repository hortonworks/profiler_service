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

package com.hortonworks.dataplane.profilers.atlas.interactors

import com.hortonworks.dataplane.profilers.atlas.AtlasApiService
import com.hortonworks.dataplane.profilers.atlas.models.CustomModels._
import com.hortonworks.dataplane.profilers.atlas.models.TagStatus
import com.hortonworks.dataplane.profilers.worker.TableResult
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.Future

case class HiveTable(database: String, table: String) {
  override def toString: String = s"$database.$table"
}

case class ProcessedTableResult(toPersistInDwh: Map[HiveTable, List[TableResultWithGuid]]
                                , toCommitInAtlas: Map[HiveTable, List[TableResultWithGuid]])

import scala.concurrent.ExecutionContext.Implicits.global

object TagProcessor extends LazyLogging {


  def mergeWithAtlas(atlasService: AtlasApiService, tableToResultsFromProfiler: Map[HiveTable, List[TableResult]], clusterName: String): Future[ProcessedTableResult] = {
    val fullyQualifiedNamesOfTables = tableToResultsFromProfiler.keys.map(table => s"${table.database}.${table.table}@$clusterName")
    val tableResultsFromAtlas: Future[Map[HiveTable, List[ColumnAndLabels]]] = atlasService.retrieveSimpleEntities(fullyQualifiedNamesOfTables.toList).flatMap(
      simpleEntityResponse => {
        val guids = simpleEntityResponse.entities.map(_.guid)
        atlasService.retrieveDetailedEntityResponse(guids).map(
          convertToTableAndLabels
        )
      }
    )
    tableResultsFromAtlas.map(x => {
      mergeTags(x, tableToResultsFromProfiler)
    }
    )

  }


  def convertToTableResults(resultsFromAtlas: List[ColumnAndLabels], table: HiveTable): List[TableResultWithGuid] = resultsFromAtlas.flatMap(
    columnAndLabels =>
      columnAndLabels.labels.map(
        label => {
          val result = TableResult(table.database, table.table
            , columnAndLabels.column, label.label, label.percentageMatch, label.status)
          TableResultWithGuid(columnAndLabels.guid, result)
        }
      )

  )

  private def mergeTags(tableToResultsFromAtlas: Map[HiveTable, List[ColumnAndLabels]]
                        , tableToResultsFromProfiler: Map[HiveTable, List[TableResult]]): ProcessedTableResult = {
    val tableAndResults: List[(HiveTable, List[TableResultWithGuid], List[TableResultWithGuid])] = tableToResultsFromProfiler.map(
      tableAndResults => {
        tableToResultsFromAtlas.get(tableAndResults._1) match {
          case Some(resultsFromAtlas) =>
            val tagsFromAtlas: Set[(String, String)] = resultsFromAtlas.flatMap(x => x.labels.map(x.column -> _.label)).toSet
            val columnToGuidMapping = getColumnToGuidMapping(resultsFromAtlas)
            val filtersResultsFromProfiler: List[TableResult] = tableAndResults._2.filter(
              x => isTagStatusSuggested(x) &&
                isTagDoesnotExistsInAtlas(x, tagsFromAtlas))
            val profilerResultsWithGuids: List[TableResultWithGuid] = filtersResultsFromProfiler.flatMap(x => {
              columnToGuidMapping.get(x.column) match {
                case Some(guid) =>
                  Some(TableResultWithGuid(guid, x))
                case None =>
                  logger.warn(s"Failed to find column ${x.column} of table ${tableAndResults._1} in Atlas")
                  None
              }
            })
            val mergedTableResults: List[TableResultWithGuid] = profilerResultsWithGuids ::: convertToTableResults(resultsFromAtlas, tableAndResults._1)
            Some((tableAndResults._1, profilerResultsWithGuids, mergedTableResults))
          case None =>
            logger.warn(s"Failed to find table ${tableAndResults._1} in Atlas")
            None
        }
      }
    ).toList.flatten
    val resultsToPersistInAtlas = tableAndResults.map(x => x._1 -> x._2).toMap
    val resultsToPersistInDwh = tableAndResults.map(x => x._1 -> x._3).toMap
    ProcessedTableResult(resultsToPersistInDwh, resultsToPersistInAtlas)
  }

  private def getColumnToGuidMapping(results: List[ColumnAndLabels]): Map[String, String] = {
    results.map(entry => entry.column -> entry.guid).toMap
  }


  private def isTagDoesnotExistsInAtlas(x: TableResult, tagsFromAtlas: Set[(String, String)]): Boolean = {
    !tagsFromAtlas.contains(x.column -> x.label)
  }

  private def isTagStatusSuggested(x: TableResult) = {
    x.status == TagStatus.suggested.toString
  }


  private def convertToTableAndLabels(tableEntities: List[TableEntity]): Map[HiveTable, List[ColumnAndLabels]] = {
    def columnName(qualifiedName: String) = qualifiedName.split("@").head.split("\\.").last

    def tableName(qualifiedName: String) = qualifiedName.split("@").head.split("\\.").last

    def dbName(qualifiedName: String) = qualifiedName.split("@").head.split("\\.").head

    tableEntities.map(
      entity => {
        val table = tableName(entity.tableEntity.qualifiedName)
        val db = dbName(entity.tableEntity.qualifiedName)
        val columns = entity.columnEntities.map(
          me => {
            val column = columnName(me.qualifiedName)
            val labels = me.tags.map(
              tag => {
                Label(tag.name, tag.status, 100)
              }
            )
            ColumnAndLabels(column, me.guid, labels)
          }
        )
        HiveTable(db, table) -> columns
      }).toMap
  }

}

