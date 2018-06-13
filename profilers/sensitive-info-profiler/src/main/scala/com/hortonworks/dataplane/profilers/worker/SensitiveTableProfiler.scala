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
package com.hortonworks.dataplane.profilers.worker


import com.hortonworks.dataplane.profilers.SensitiveLabelProfiler
import com.hortonworks.dataplane.profilers.atlas.interactors.HiveTable
import com.hortonworks.dataplane.profilers.atlas.models.TagStatus
import com.hortonworks.dataplane.profilers.keyword.KeyWordMatch
import com.hortonworks.dataplane.profilers.label.{LabelBuilder, TableResult}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.io.Source

object SensitiveTableProfiler extends LazyLogging {


  private val keywordStore: String = Source.fromInputStream(getClass.getResourceAsStream("/keywords.json")).mkString
  private val labelStore: String = Source.fromInputStream(getClass.getResourceAsStream("/labelstore.json")).mkString
  private val matcher: KeyWordMatch = KeyWordMatch.fromJson(keywordStore)
  private val auxiliaryAttributes: Map[String, Map[String, String]] = LabelBuilder.buildAuxiliaryAttributes(labelStore)

  private def getDataQuery(hiveTable: HiveTable, sampleSize: Long) = {
    s"select * from ${hiveTable.database}.${hiveTable.table} " +
      s"distribute by rand() sort by rand() limit $sampleSize"
  }

  /*
85% percent weight to values in column and 15% weight to keyword match in column name
 */
  private def weightedScore(percentMatch: Double, keyWordMatch: Boolean, label: String) = {

    val colNameWeight = auxiliaryAttributes(label).getOrElse("colNameWeight","0").toDouble
    val regexWeight = auxiliaryAttributes(label).getOrElse("regexWeight","0").toDouble

    val percentMatchWeight = (regexWeight/100) * percentMatch

    logger.debug(s"label=$label , colNameWeight=$colNameWeight , regexWeight=$regexWeight, percentMatchWeight=$percentMatchWeight, keyWordMatch=$keyWordMatch")

    if (keyWordMatch) {
      percentMatchWeight + colNameWeight
    } else {
      percentMatchWeight
    }
  }

  def profile(sparkSession: SparkSession,
              table: HiveTable, sampleSize: Long,
              sensitiveLabelProfiler: SensitiveLabelProfiler): List[TableResult] = {
    val data = sparkSession.sql(getDataQuery(table, sampleSize))
    if (data.rdd.isEmpty()) {
      List.empty[TableResult]
    }
    else {

      data.persist(StorageLevel.MEMORY_ONLY)
      val columnKeywordMatches = keywordMatches(sparkSession, table.database, table.table)
      sensitiveLabelProfiler.profile(data)
        .map(r => {

          val percent = if(r.total == r.nullCount) 0
          else (r.count * 100) / (r.total - r.nullCount)

          TableResult(table.database, table.table, r.name, r.label,percent, TagStatus.suggested.toString)
        })
        .filter(entry => weightedScore(entry.percentMatch, columnKeywordMatches.getOrElse(entry.column, List()).
            contains(entry.label), entry.label) > 70
        ).toList
    }
  }


  private def keywordMatches(sparkSession: SparkSession, dbName: String, tableName: String) = {
    val columnList = sparkSession.sql("describe " + dbName + "." + tableName).collect().map(entry =>
      entry(0).toString)
    columnList.map(colName => {
      val keywordList = matcher.search(colName).map(x => x._2).toList
      (colName, keywordList)
    }).toMap
  }

}
