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
package com.hortonworks.dataplane.profilers

import java.sql.Date
import java.text.SimpleDateFormat
import java.util.Calendar

import com.hortonworks.dataplane.profilers.hdpinterface.spark.SparkInterface
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import scala.util.Try

object HiveTableMetadata extends LazyLogging {

  private val Unknown = "UNKNOWN"

  def attributeFilter(attr: String): Boolean = {
    return List("Statistics", "Created", "Database", "InputFormat", "OutputFormat", "Table Properties", "Created Time").contains(attr)
  }

  def getNumPartitions(sparkInterface: SparkInterface, tableName: String, dbName: String): Long = {
    var partitionCount = 0L
    try {
      partitionCount = sparkInterface.execute("show partitions " + dbName + "." + tableName).count()
    } catch {
      case e: Exception =>
    }
    partitionCount
  }

  def getTableStats(hiveDefaultStats: String, sparkStats: String): (Long, Long) = {
    val hiveTableStatsMap = hiveDefaultStats.replaceAll("\\[", "").replaceAll("\\]", "").split(",").
      map(x => {
        val values = x.split("=")
        if (values.size == 2) {
          Some((values(0).trim -> values(1)))
        } else None
      }).filter(_.isDefined).map(_.get).toMap
    val sparkStatsEntries = sparkStats.split(" ")
    val (sparkNumRows, sparkTotalSize) = if (sparkStatsEntries.size > 2) {
      (sparkStatsEntries(2).toLong, sparkStatsEntries(0).toLong)
    } else (0, 0)
    val numRows = hiveTableStatsMap.getOrElse("numRows", sparkNumRows).toString.toLong
    val totalSize = hiveTableStatsMap.getOrElse("totalSize", sparkTotalSize).toString.toLong
    (totalSize, numRows)
  }

  def getRDDWithMetadata(sparkInterface: SparkInterface, tableName: String, dbName: String): RDD[Row] = {
    logger.info("Constructing RDD row for table {}.{}", dbName, tableName)
    val filteredMetadataMap = sparkInterface.execute("describe formatted " + dbName + "." + tableName).
      filter(x => attributeFilter(x(0).toString)).collect().map(x => (x(0).toString -> x(1).toString)).toMap
    val numPartitions = getNumPartitions(sparkInterface, tableName, dbName)
    val session: SparkSession = sparkInterface.session
    import session.implicits._
    val stats = getTableStats(filteredMetadataMap.getOrElse("Table Properties", Unknown), filteredMetadataMap.getOrElse("Statistics", "0 0 0"))
    val date =
      Try(Utils.formatDate(filteredMetadataMap.getOrElse("Created", filteredMetadataMap("Created Time")))).getOrElse(new Date(0))
    val rdd = Seq((Utils.getTableQualifiedName(tableName, dbName), filteredMetadataMap.getOrElse("Database", Unknown),
      date, stats._1, stats._2, numPartitions,
      filteredMetadataMap.getOrElse("InputFormat", Unknown), filteredMetadataMap.getOrElse("OutputFormat", Unknown))).toDF("tableQualifiedName", "db", "createTime", "size",
      "numRows", "numPartitions", "inputFormat", "outputFormat").rdd
    rdd
  }

  def collectMetadata(sparkInterface: SparkInterface): Dataset[Row] = {
    val schema = Array(("table", StringType), ("db", StringType), ("createTime", DateType),
      ("size", LongType), ("numRows", LongType), ("numPartitions", LongType),
      ("inputFormat", StringType), ("outputFormat", StringType))
    val schemaRDD = StructType(schema.map(field => StructField(field._1, field._2, true)))
    val databases = sparkInterface.showDatabases()
    val databaseList = databases.collect()
    val fullMetadata = databaseList.flatMap(row => {
      val database = row(0).toString()
      val tables = sparkInterface.showTables(database)
      val tableMetadataData = tables.collect().map(r => {
        val tableName = r(0).toString
        logger.info("Collecting metadata for table {}.{}", database, tableName)
        try {
          Some(getRDDWithMetadata(sparkInterface, tableName, database))
        } catch {
          case e: Exception =>
            logger.error(s"Error while processing table $database.$tableName", e)
            None
        }
      }).filter(_.isDefined).map(_.get)
      tableMetadataData
    })
    logger.info("RDDs for all tables computed. Now merging")
    val mergedRDD = sparkInterface.session.sparkContext.union(fullMetadata)
    logger.info("Converting RDD to Dataframe and reducing the number of partitions")
    sparkInterface.session.sqlContext.createDataFrame(mergedRDD, schemaRDD).coalesce(5)
  }

  def writeResultsToHDFS(filePath: String, mergedDF: Dataset[Row]) = {
    mergedDF.cache()
    val today: String = {
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val cal = Calendar.getInstance
      dateFormat.format(cal.getTime)
    }
    val optionalSeperator = addSeperatorToPath(filePath)
    val partitionedPath = filePath + s"${optionalSeperator}daily/date=$today"
    val snapshotPath = filePath + s"${optionalSeperator}snapshot"
    mergedDF.write.mode(SaveMode.Overwrite).parquet(partitionedPath)
    mergedDF.write.mode(SaveMode.Overwrite).parquet(snapshotPath)
  }

  private def addSeperatorToPath(filePath: String) = {
    if (filePath.endsWith("/")) "" else "/"
  }

}