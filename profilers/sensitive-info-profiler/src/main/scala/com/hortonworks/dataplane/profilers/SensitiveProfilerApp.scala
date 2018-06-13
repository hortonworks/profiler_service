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

import com.hortonworks.dataplane.profilers.atlas.interactors.{HiveTable, ProcessedTableResult, TagProcessor}
import com.hortonworks.dataplane.profilers.commons.Constants
import com.hortonworks.dataplane.profilers.label.TableResult
import com.hortonworks.dataplane.profilers.worker.SensitiveTableProfiler
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Dataset, SaveMode}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

object SensitiveProfilerApp extends App with LazyLogging {

  val config = new AppConfig(args)

  val services = new AppServices(config.input.atlasInfo)


  createHdfsPathsIfNotExists()
  createDpTag()
  createMissingColumnTagTypes()
  profilerTables()
  refreshSnapshotData()

  def createHdfsPathsIfNotExists() = {
    val partitionedPath = s"${config.input.path}/${config.pathPartitionedSuffix}"
    val aggregatePath = s"${config.input.path}/${config.pathSuffix}"
    val dirs = List(partitionedPath, aggregatePath)
    createDirectoriesInHdfs(dirs)
  }


  private def profilerTables() = {
    config.input.tables.foreach {
      t => processATable(t.db, t.table)
    }
  }


  private def processATable(dbName: String, tableName: String): Unit = {

    val table = HiveTable(dbName, tableName)
    val eventualTableResults = Future(SensitiveTableProfiler.profile(config.sparkSession, table
      , config.input.sampleSize, services.sensitiveLabelProfiler))
    val eventualResult: Future[ProcessedTableResult] = eventualTableResults.flatMap(
      tableResults =>
        TagProcessor.mergeWithAtlas(services.atlasApiService,
          Map(table -> tableResults), config.input.atlasInfo.clusterName)
    )

    val eventualTagUpdate = eventualResult.flatMap(
      processedTableResult => {
        services.atlasApiService.persistTags(processedTableResult.toCommitInAtlas.values.flatten.toList, retry = 3)
          .map(_ => processedTableResult)
      }
    ).map(
      processedTableResult => {
        import config.sparkSession.implicits._
        val savePath = s"${config.input.path}/${config.pathPartitionedSuffix}/database=$dbName/table=$tableName"
        val dataToPersistInDwh: Dataset[TableResult] =
          processedTableResult.toPersistInDwh.values.flatten.map(x => appendDpPrefixToTag(x.tableResult)).toList.toDS()
        dataToPersistInDwh.write.mode(SaveMode.Overwrite).parquet(savePath)
      }
    )
    waitForALongPeriodForThisJobToComplete(table, eventualTagUpdate, 60 * 60 * 24)
  }

  private def appendDpPrefixToTag(tableResult: TableResult): TableResult = {

    val labelWithDpPrefix = Constants.dpTagPrefix + tableResult.label
    tableResult.copy(label = labelWithDpPrefix)
  }

  private def waitForALongPeriodForThisJobToComplete(table: HiveTable,
                                                     eventualTagUpdate: Future[Unit], timeOutInSeconds: Int) = {
    Await.ready(eventualTagUpdate, timeOutInSeconds seconds).value match {
      case Some(mayBeResult) =>
        mayBeResult match {
          case Success(_) =>
            logger.debug(s"table  $table was successfully profiled")
          case Failure(error) =>
            logger.error(s"Failed to profiler table $table ", error)
        }

      case None =>
        logger.error(s"profiling of table  $table timed out, waited for $timeOutInSeconds seconds")
    }
  }

  def doesHdfsDirectoryHasData(path: String) = {
    config.hdfsFileSystem.listStatus(new Path(path)).length > 0
  }

  private def refreshSnapshotData() = {
    val path = config.input.path
    val aggregateSavePath = s"$path/${config.pathSuffix}"
    val partitionedPath = s"$path/${config.pathPartitionedSuffix}"
    if (doesHdfsDirectoryHasData(partitionedPath)) {
      config.sparkSession.read.parquet(partitionedPath).write.mode(SaveMode.Overwrite).parquet(aggregateSavePath)
    }
    else
      logger.warn(s"Found no files at partitioned path : $partitionedPath, " +
        s"Cannot refresh aggregate data at $aggregateSavePath")
  }


  private def createDpTag() = {
    withRetry(services.tagCreatorService.createDPTagIfNotExists())
  }

  private def createMissingColumnTagTypes() = {
    val allTags = services.sensitiveLabelProfiler.getAllLabels()
    Await.result(
      services.atlasApiService.getDpTags().map(
        existingTags => {
          val newTags: Set[String] = allTags.toSet.filter(!existingTags.contains(_))
          withRetry(services.tagCreatorService.createColumnTags(newTags.toSeq))
        }
      ), 60 seconds)
  }


  private def createDirectoriesInHdfs(dirs: List[String]) = {
    dirs.foreach(fullPath => {
      val path = new Path(fullPath)
      if (!config.hdfsFileSystem.exists(path))
        config.hdfsFileSystem.mkdirs(path)
    })
  }


  private def withRetry[T](block: => Try[T], retries: Int = 3, waitTimeMs: Long = 5000): T = {
    block match {
      case Success(t) =>
        logger.debug("Tag attachment successful {}", t.toString)
        t
      case Failure(th) =>
        logger.debug("Tag attachment failed with exception {}", th.getMessage)
        if (retries > 0) {
          logger.debug("Retrying again: Retries left : {}", (retries - 1).toString)
          Thread.sleep(waitTimeMs)
          withRetry(block, retries - 1)
        } else throw th
    }
  }


}