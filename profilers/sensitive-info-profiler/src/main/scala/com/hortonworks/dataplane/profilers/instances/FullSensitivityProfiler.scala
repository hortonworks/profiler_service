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


package com.hortonworks.dataplane.profilers.instances

import com.hortonworks.dataplane.profilers.atlas.interactors.{HiveTable, ProcessedTableResult, TagProcessor}
import com.hortonworks.dataplane.profilers.commons.{AppConfig, AppServices, Constants}
import com.hortonworks.dataplane.profilers.kraptr.dsl.TagCreator
import com.hortonworks.dataplane.profilers.worker.{TableResult, TagIdentifier}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}


class FullSensitivityProfiler(config: AppConfig) extends LazyLogging with SensitivityProfiler {
  val services = new AppServices(config.input.atlasInfo)


  def triggerProfiler(): Unit = {
    createHdfsPathsIfNotExists()
    createDpTag().flatMap(
      _ => profileTablesDecideToRefreshSnapshotData().map(
        shouldIRefreshSnapshot => {
          if (shouldIRefreshSnapshot) {
            refreshSnapshotData()
          }
        })
    ).map(
      _ => config.sparkInterface.close()
    ).get
  }


  private def createHdfsPathsIfNotExists() = {
    val partitionedPath = s"${config.input.path}/${config.pathPartitionedSuffix}"
    val aggregatePath = s"${config.input.path}/${config.pathSuffix}"
    val dirs = List(partitionedPath, aggregatePath)
    createDirectoriesInHdfs(dirs)
  }


  private def profileTablesDecideToRefreshSnapshotData(): Try[Boolean] = {
    config.tagCreatorsAndTag match {
      case Success(tagCreatorAndSchema) =>
        val allTags = tagCreatorAndSchema.flatMap(_.tags).toSet
        if (allTags.isEmpty) {
          logger.error("No tags to identify. Skipping sensitivity profiler run")
          Success(false)
        }
        else {
          createMissingColumnTagTypes(allTags).map(
            _ => {
              config.input.tables.foreach {
                t =>
                  processATable(t.db, t.table,
                    tagCreatorAndSchema)
              }
              true
            }
          )
        }
      case Failure(exception) =>
        val errorMessage = "Failed to load dsl configs"
        logger.error(errorMessage, exception)
        Failure(new Exception(errorMessage, exception))
    }
  }


  private def processATable(dbName: String,
                            tableName: String,
                            tagCreators: List[TagCreator]): Unit = {

    val table = HiveTable(dbName, tableName)
    val eventualTableResults: Future[List[TableResult]] = Future.fromTry(TagIdentifier.profile(config.sparkInterface, table,
      config.input.sampleSize, tagCreators))
    val eventualResult: Future[ProcessedTableResult] = eventualTableResults.flatMap(
      tableResults => {
        logger.info(s"Sensitive labels adjusted for threshold on  $table are $tableResults")
        TagProcessor.mergeWithAtlas(services.atlasApiService,
          Map(table -> tableResults), config.input.atlasInfo.clusterName)
      }
    )

    val eventualTagUpdate = eventualResult.flatMap(
      processedTableResult => {
        services.atlasApiService.persistTags(processedTableResult.toCommitInAtlas.values.flatten.toList, retry = 3)
          .map(_ => processedTableResult)
      }
    ).map(
      processedTableResult => {
        val session: SparkSession = config.sparkInterface.session
        import session.implicits._
        val savePath = s"${config.input.path}/${config.pathPartitionedSuffix}/database=$dbName/table=$tableName"
        val dataToPersistInDwh: Dataset[TableResult] =
          processedTableResult.toPersistInDwh.values.flatten.
            map(x => appendDpPrefixToTag(x.tableResult)).toList.distinct.toDS()
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

  private def doesHdfsDirectoryHasData(path: String) = {
    config.hdfsFileSystem.listStatus(new Path(path)).length > 0
  }

  private def refreshSnapshotData() = {
    val path = config.input.path
    val aggregateSavePath = s"$path/${config.pathSuffix}"
    val partitionedPath = s"$path/${config.pathPartitionedSuffix}"
    if (doesHdfsDirectoryHasData(partitionedPath)) {
      Try(config.sparkInterface.session.read.parquet(partitionedPath).write.mode(SaveMode.Overwrite).parquet(aggregateSavePath)) match {
        case Failure(exception) =>
          logger.error("Failed to update snapshot data (Probably a race condition)", exception)
        case _ => // Do nothing
      }
    }
    else
      logger.warn(s"Found no files at partitioned path : $partitionedPath, " +
        s"Cannot refresh aggregate data at $aggregateSavePath")
  }


  private def createDpTag(): Try[String] = {
    withRetry("Create dp tag", services.tagCreatorService.createDPTagIfNotExists())
  }

  private def createMissingColumnTagTypes(tags: Set[String]): Try[String] = {
    val createTags = services.atlasApiService.getDpTags().flatMap(
      existingTags => {
        val newTags: Set[String] = tags.filter(!existingTags.contains(_))
        Future.fromTry(withRetry("Create column tags",
          services.tagCreatorService.createColumnTags(newTags.toSeq)))
      }
    )
    Await.ready(createTags, 600 seconds).value.getOrElse(Failure(
      new Exception("Could not create tags in atlas after 600 seconds")
    ))
  }


  private def createDirectoriesInHdfs(dirs: List[String]) = {
    dirs.foreach(fullPath => {
      val path = new Path(fullPath)
      if (!config.hdfsFileSystem.exists(path))
        config.hdfsFileSystem.mkdirs(path)
    })
  }


  private def withRetry[T](operationName: String, block: => Try[T], retries: Int = 3, waitTimeMs: Long = 5000): Try[T] = {
    block recoverWith {
      case error: Throwable if retries > 0 =>
        logger.warn("Tag attachment failed with exception {}", error)
        withRetry(operationName, block, retries - 1)
      case error: Throwable => Failure(new Exception(s"Failed to run $operationName after allowed number of retries", error))
    }
  }

}
