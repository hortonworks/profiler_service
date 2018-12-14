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

import java.lang

import com.hortonworks.dataplane.profilers.atlas.interactors.HiveTable
import com.hortonworks.dataplane.profilers.atlas.models.TagStatus
import com.hortonworks.dataplane.profilers.hdpinterface.spark.SparkInterface
import com.hortonworks.dataplane.profilers.kraptr.dsl.TagCreator
import com.hortonworks.dataplane.profilers.kraptr.models.dsl.MatchType
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.LongAccumulator

import scala.collection.immutable
import scala.util.{Success, Try}

object TagIdentifier extends LazyLogging {

  val confidenceThreshold: Double = 70.0d


  private def getDataQuery(hiveTable: HiveTable, sampleSize: Long) = {
    s"select * from ${hiveTable.database}.${hiveTable.table} " +
      s"distribute by rand() sort by rand() limit $sampleSize"
  }


  def profile(sparkInterface: SparkInterface,
              table: HiveTable, sampleSize: Long
              , tagCreators: List[TagCreator]): Try[List[TableResult]] = {
    val possibleData: Try[DataFrame] = Try(sparkInterface.executeQuery(getDataQuery(table, sampleSize)))
    possibleData.flatMap {
      data =>
        if (data.rdd.isEmpty()) {
          Success(List.empty[TableResult])
        }
        else {
          val nameMatchers: List[TagCreator] = tagCreators.filter(_.matchType == MatchType.name)
          val columnNameMatchers: Seq[((String, String), Double)] = findColumnNameMatches(data.columns, nameMatchers, sparkInterface.session)
          logger.info(s"tags identified on $table from column names are $columnNameMatchers")
          val valueMatchers: List[TagCreator] = tagCreators.filter(_.matchType == MatchType.value)
          val columnValueMatchers: Seq[((String, String), Double)] = getColumnValueMatches(data, table,
            valueMatchers, sparkInterface.session)
          logger.info(s"tags identified on $table from column values are $columnValueMatchers")
          val allTagsDetected: Seq[((String, String), Double)] = columnValueMatchers ++ columnNameMatchers
          val aggregatedTagsAndTheirConfidence: Map[(String, String), Double] = allTagsDetected.groupBy(_._1).mapValues(_.map(_._2).sum)
          val unAdjustedTableResults: immutable.Iterable[TableResult] = aggregatedTagsAndTheirConfidence.map(x => TableResult(table.database, table.table, x._1._1, x._1._2, x._2, TagStatus.suggested.toString))
          logger.info(s"Sensitive labels identified on $table are $unAdjustedTableResults")
          Success(unAdjustedTableResults.filter(_.confidence > 70).toList)
        }
    }
  }


  private case class ColumnTag(column: String, tag: String, count: Long)


  private def getColumnValueMatches(data: DataFrame,
                                    hiveTable: HiveTable,
                                    valueMatchers: List[TagCreator],
                                    sparkSession: SparkSession): Seq[((String, String), Double)] = {
    val sampleSizAccumulator: LongAccumulator = sparkSession.sparkContext
      .longAccumulator(s"sample_count_$hiveTable")
    val columns: Array[String] = data.columns
    val indices: Range = columns.indices

    val columnTagsAndConfidence: RDD[(String, Option[Map[String, Double]])] = data.rdd.flatMap(
      row => {
        sampleSizAccumulator.add(1)
        indices.map(
          index => {
            val columnName: String = columns(index)
            if (row.isNullAt(index)) {
              columnName -> None
            }
            else {
              val columnValue: String = row.get(index).toString
              val tagToConfidence: Map[String, Double] = valueMatchers.flatMap(
                matcher =>
                  matcher.apply(columnValue).map(x => x -> matcher.confidence)
              ).groupBy(_._1).mapValues(_.map(_._2).max)
              columnName -> Some(tagToConfidence)
            }
          }
        )

      }
    )
    val columnsAndNullCount: Map[String, Long] = columnTagsAndConfidence.filter(_._2.isEmpty).map(_._1 -> 1l).reduceByKey(_ + _).collect().toMap
    val sampleSize: lang.Long = sampleSizAccumulator.value
    val columnAndTagsAggregatedConfidence: Array[((String, String), Double)] = columnTagsAndConfidence.filter(_._2.isDefined)
      .mapValues(_.get)
      .flatMap(x => x._2.map(y => (x._1 -> y._1) -> y._2))
      .reduceByKey(_ + _).collect()

    columnAndTagsAggregatedConfidence.flatMap(x => {
      val nullCount = columnsAndNullCount.getOrElse(x._1._1, 0l)
      if (nullCount == sampleSize) {
        None
      }
      else {
        Some(x._1._1 -> x._1._2 -> x._2 / (sampleSize - nullCount))
      }

    })
  }


  private def findColumnNameMatches(columns: Array[String],
                                    nameMatchers: List[TagCreator],
                                    sparkSession: SparkSession): Seq[((String, String), Double)] = {
    val columnsAndTags: Map[String, Seq[(String, Double)]] = columns.map(
      column => {
        val tagAndConfidenceScore: List[(String, Double)] = nameMatchers.flatMap(matcher => {
          val tuples = matcher.apply(column).map(x => x -> matcher.confidence)
          tuples
        })
        column -> tagAndConfidenceScore
      }
    ).toMap


    val columnsAndAdjustedTags: Map[String, Map[String, Double]] = columnsAndTags.mapValues(
      tagsAndConfidence =>
        tagsAndConfidence.groupBy(_._1).mapValues(_.map(_._2).max)
    )
    columnsAndAdjustedTags.flatMap(x => x._2.map(y => (x._1 -> y._1) -> y._2)).toSeq
  }


}
