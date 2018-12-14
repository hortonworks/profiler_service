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

package com.hortonworks.dataplane.profilers.tablestats

import com.hortonworks.dataplane.profilers.commons.parse.HiveAsset
import com.hortonworks.dataplane.profilers.hdpinterface.spark.SparkInterface
import com.hortonworks.dataplane.profilers.tablestats.aggregators._
import com.hortonworks.dataplane.profilers.tablestats.utils._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

import scala.collection.JavaConverters._
import scala.util.Try


object TableStats extends LazyLogging {

  implicit val formats = org.json4s.DefaultFormats

  val sparkToHiveDataTypeMap = Map(("StringType" -> "string"), ("BooleanType" -> "boolean"),
    ("DoubleType" -> "double"), ("DecimalType" -> "decimal"), ("FloatType" -> "float"),
    ("LongType" -> "long"), ("IntegerType" -> "int"))

  def getTableDF(sparkInterface: SparkInterface, database: String, table: String, samplePercent: Double) = {

    val hive_query = "select * from " + database + "." + table
    logger.info("Hive Query : " + hive_query)

    val tableDF = sparkInterface.executeQuery(hive_query)
    val sampleFraction = samplePercent.toDouble / 100.0


    val sampledDF = if (samplePercent == 100) tableDF
    else tableDF.sample(false, sampleFraction)

    sampledDF
  }

  def getTypes(dataFrame: DataFrame) = {
    val colsWithTypes = AggregatorHelper.getColumnsWithType(dataFrame)

    val columnTypes = colsWithTypes.map(col => new ColumnMetrics(col._1,
      new ColumnTypeMetrics(sparkToHiveDataTypeMap.getOrElse(col._2.toString, col._2.toString))))
    new MetricsSeq(columnTypes)
  }

  def profileDFFull(tableDF: DataFrame) = {

    val metrics: MetricsSeq = AggregatorHelper.getAggregators(tableDF)
      .execute(tableDF.rdd).asInstanceOf[MetricsSeq]

    val frequentCountAndHistogramMetrics = AggregatorHelper.getFrequentCountAndHistogramAggregators(tableDF, metrics.seq)
      .execute(tableDF.rdd).asInstanceOf[MetricsSeq]

    val typeNames: MetricsSeq = getTypes(tableDF)
    new MetricsSeq(metrics.seq ++ typeNames.seq ++ frequentCountAndHistogramMetrics.seq)
  }

  def profileDFPerPartition(sparkInterface: SparkInterface, ti: HiveAsset, samplePercent: Double, savePath: String) = {

    val dbName = ti.db
    val tableName = ti.table
    val partitions = ti.partitions

    val tableDF = getTableDF(sparkInterface, dbName, tableName, samplePercent)

    val typeNames: MetricsSeq = getTypes(tableDF)

    //get partitions
    val partitionsForQuery: List[String] = getPartitions(dbName, tableName, partitions, sparkInterface)
    logger.info("Below are the partitions for table " + dbName + "." + tableName)
    if (partitionsForQuery.isEmpty) logger.info("table " + dbName + "." + tableName + " is not a partitioned table") else partitionsForQuery.foreach(logger.info(_))

    val metricSeq = partitionsForQuery match {
      case Nil => profileDFFull(tableDF)
      case _ => processPartitions(sparkInterface, dbName, tableName, partitionsForQuery, savePath)
    }

    new MetricsSeq(typeNames.seq ++ metricSeq.seq)
  }

  def processPartitions(sparkInterface: SparkInterface, dbName: String, tableName: String, partitions: List[String], savePath: String): MetricsSeq = {

    val partitionsResultFunc: (List[StateSeqPerPartition], String) => List[StateSeqPerPartition] = getPartitionsResult(sparkInterface, dbName, tableName)
    val partitionResults: List[StateSeqPerPartition] = partitions.foldLeft(List.empty[StateSeqPerPartition])(partitionsResultFunc)

    val dsToSave = performDatasetOperation(sparkInterface, dbName, tableName, savePath, partitions, partitionResults)

    getMetricsFromDataset(sparkInterface, dsToSave)
  }

  def getMetricsFromDataset(sparkInterface: SparkInterface, ds: Dataset[StateSeqPerPartition]) = {

    val session: SparkSession = sparkInterface.session
    import session.implicits._

    val accumulatorList = ds.as[StateSeqPerPartition].collectAsList().asScala.toList.map(t => t.colCombStates)
    val resMap: Map[String, ColumnCombinedAccumulator] = accumulatorList.foldLeft(Map.empty[String, ColumnCombinedAccumulator])(mergeColumnCombState)
    val resStateSeq = StateSeq(resMap.values.map(t => t.toStates).flatten.toSeq)
    resStateSeq.toMetrics
  }

  def performDatasetOperation(sparkInterface: SparkInterface, dbName: String, tableName: String, savePath: String, partitions: List[String], partitionResults: List[StateSeqPerPartition]) = {

    val session: SparkSession = sparkInterface.session
    import session.implicits._


    val tableSavePath = s"$savePath/database=$dbName/table=$tableName"
    val tableTmpPath = s"$savePath/tmp/database=$dbName/table=$tableName"

    val existingDS = Try {
      val frame = sparkInterface.session.read.parquet(tableSavePath)
      frame.as[StateSeqPerPartition]
    }.getOrElse(sparkInterface.session.emptyDataset[StateSeqPerPartition])

    val filteredDS = existingDS.filter(!$"partition".isin(partitions: _*))
    val partitionResultDS = partitionResults.toDS()
    val dsToSaveTmp = filteredDS.union(partitionResultDS)

    dsToSaveTmp.write.mode(SaveMode.Overwrite).parquet(tableTmpPath) //Dataset has to be saved at a temp location first. Spark does not allow read and write from same location.
    val dsToSave = sparkInterface.session.read.parquet(tableTmpPath).as[StateSeqPerPartition]
    dsToSave.write.mode(SaveMode.Overwrite).parquet(tableSavePath)
    logger.info("printing dsToSave DS")
    dsToSave.show(false)
    dsToSave
  }

  def mergeColumnCombState(first: Map[String, ColumnCombinedAccumulator], second: Map[String, ColumnCombinedAccumulator]) = {
    val res: Map[String, ColumnCombinedAccumulator] = if (first.isEmpty) second else {
      first.map {
        case (col, ccAcc) => (col, ccAcc.merge(second.getOrElse(col, ColumnCombinedAccumulator(col, None, None, None, None))))
      }
    }
    res
  }

  def getPartitionsResult(sparkInterface: SparkInterface, dbName: String, tableName: String)(stateSeqPerPartition: List[StateSeqPerPartition], partition: String) = {

    val partitionDF = sparkInterface.executeQuery("select * from " + dbName + "." + tableName + " where " + partition)
    val stateSeq = AggregatorHelper.getPartitionedAggregators(partitionDF).stateExecutor(partitionDF.rdd).asInstanceOf[StateSeq]
    val mergedStateSeqPerPartition = StateSeqPerPartition(partition, stateSeq.toColumnCombinedStateMap) :: stateSeqPerPartition
    mergedStateSeqPerPartition

  }

  def getPartitions(dbName: String, tableName: String, partitions: Seq[Map[String, String]], sparkInterface: SparkInterface): List[String] = {

    val partitionsRes: List[String] = partitions.map { partMap =>
      partMap.map { case (k, v) => k + "=" + "'" + v + "'" }.mkString(" and ")
    }.toList
    if (!partitionsRes.isEmpty) partitionsRes else getPartitonsFromHiveTable(sparkInterface, dbName, tableName)

  }

  def getPartitonsFromHiveTable(sparkInterface: SparkInterface, dbName: String, tableName: String): List[String] = {

    val session: SparkSession = sparkInterface.session
    import session.implicits._

    val partitions: List[String] = try {
      val partitionDf = sparkInterface.executeQuery("show partitions " + dbName + "." + tableName)
      partitionDf.select("partition").map(r => r.mkString).collect.toList
    } catch {
      case e: Exception => Nil
    }
    partitions.map(s => s.replace("=", "='")).map(s => s.replace("/", "' and ") + "'")
  }

}
