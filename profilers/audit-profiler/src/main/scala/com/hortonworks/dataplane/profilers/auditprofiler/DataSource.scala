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

package com.hortonworks.dataplane.profilers.auditprofiler

import java.net.URI
import java.time.format.DateTimeFormatter

import com.hortonworks.dataplane.profilers.auditprofiler.AggregationType.AggregationType
import org.apache.commons.collections.IteratorUtils
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

sealed abstract class DataSource

case class RangerAuditRaw(asset: String) extends DataSource

case class AssetRollup(metric: String) extends DataSource

object DataSource {

  def getDF(sparkSession: SparkSession, job: AuditQueryJob, properties: Map[String, String]): Option[DataFrame] = {
    job.source match {
      case rangerRaw: RangerAuditRaw => getRangerAuditDF(sparkSession, job, properties)
      case assetRollup: AssetRollup => getAssetRollupDF(sparkSession, job, properties)
    }
  }

  private def dirExists(session: SparkSession) = {
    val fs = FileSystem.get(session.sparkContext.hadoopConfiguration)
    (dir: String) => fs.exists(new Path(dir))
  }

  private def getFilesForDir(session: SparkSession, dirs: Seq[String]) = {
    val fs = FileSystem.get(session.sparkContext.hadoopConfiguration)

    def getFiles(dir: String) = {
      val p = new Path(dir)
      val files = ArrayBuffer[FileStatus]()
      val fileIter = fs.listFiles(p, false)

      while (fileIter.hasNext) {
        files += fileIter.next()
      }

      files.filter(_.getLen > 10).map(_.getPath.getName).map(e => s"$dir/$e").toSeq
    }

    dirs.flatMap(getFiles)
  }

  private def getRangerAuditDF(sparkSession: SparkSession, job: AuditQueryJob, properties: Map[String, String]): Option[DataFrame] = {
    val dirs = getRangerDirToProcess(job, properties).filter(dirExists(sparkSession))
    val files = getFilesForDir(sparkSession, dirs)

    if (files.isEmpty){
      println("No file found to process.")
      None
    } else{
      println("Files to read : " + files.mkString(","))

      val frame = sparkSession.sqlContext.read.json(files: _*)
      Some(DFTransformer.transform(frame, job.source))
    }
  }

  private def getAssetRollupDF(sparkSession: SparkSession, job: AuditQueryJob, properties: Map[String, String]) = {
    val dirs = getAssetRollupDirToProcess(job, properties).filter(dirExists(sparkSession))
    Some(sparkSession.sqlContext.read.parquet(dirs: _*))
  }


  private def getBaseDirForRangerAuditRaw(source: RangerAuditRaw, properties: Map[String, String]) = {
    val basePath = if (properties.contains("rangerAuditDir")) {
      val auditUrl = properties.get("rangerAuditDir").get
      new URI(auditUrl).getPath
    } else AuditProfilerConstants.DefaultSourcePath
    s"$basePath/${source.asset}"
  }


  private def getBaseDirForAssetRollup(source: AssetRollup, aggregationType: AggregationType, properties: Map[String, String]) = {
    val basePath = properties.getOrElse("assetMetricsPath", AuditProfilerConstants.DefaultSavePath)
    s"${basePath}/${source.metric}/${AggregationType.Daily.toString.toLowerCase()}"
  }

  def getRangerDateDirs(job: AuditQueryJob) = {
    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    val days = AggregationType.getDates(job.aggregationType, job.endDate)
    days.map(formatter.format)
  }

  def getAssetRollupDateDirs(job: AuditQueryJob) = {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val days = AggregationType.getDates(job.aggregationType, job.endDate)
    days.map(formatter.format)
  }

  private def getAssetRollupDirToProcess(job: AuditQueryJob, properties: Map[String, String]) = {
    val baseDir = getBaseDirForAssetRollup(job.source.asInstanceOf[AssetRollup], job.aggregationType, properties)
    getAssetRollupDateDirs(job).map(date => s"$baseDir/date=$date")
  }

  private def getRangerDirToProcess(job: AuditQueryJob, properties: Map[String, String]) = {
    val baseDir = getBaseDirForRangerAuditRaw(job.source.asInstanceOf[RangerAuditRaw], properties)
    getRangerDateDirs(job).map(date => s"$baseDir/$date")
  }
}


