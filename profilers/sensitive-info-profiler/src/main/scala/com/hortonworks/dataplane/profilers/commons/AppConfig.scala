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

package com.hortonworks.dataplane.profilers.commons

import com.hortonworks.dataplane.profilers.commons.Models.{DryRunSettings, InputArgs}
import com.hortonworks.dataplane.profilers.commons.parse.ProfilerInput
import com.hortonworks.dataplane.profilers.hdpinterface.impl.spark.SparkInterfaceImpl
import com.hortonworks.dataplane.profilers.hdpinterface.spark.SparkInterface
import com.hortonworks.dataplane.profilers.kraptr.common.KraptrContext
import com.hortonworks.dataplane.profilers.kraptr.dsl.TagCreator
import com.hortonworks.dataplane.profilers.kraptr.engine.KraptrEngine
import com.hortonworks.dataplane.profilers.kraptr.vfs.hdfs.{HDFSFile, HDFSFileSystem}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.FileSystem
import play.api.libs.json.Json

import scala.util.Try

class AppConfig(args: Seq[String]) extends LazyLogging {
  val pathPartitionedSuffix = "/hivesensitivitypartitioned/snapshot"
  val pathSuffix = "/hivesensitivity/snapshot"
  val input: InputArgs = parseInput(args.head)

  logger.debug("Args : {}", args.head)
  logger.debug("Input : {}", input.toString)


  val sparkInterface: SparkInterface = new SparkInterfaceImpl("SensitiveInfoProfiler", input.profilerInput.sparkConfigurations.toSeq)


  val hdfsFileSystem: FileSystem = sparkInterface.getFileSystem

  lazy val dslContext = KraptrContext(input.configRootPath,
    input.profilerInput.context.profilerinstancename,
    HDFSFileSystem(hdfsFileSystem),
    sparkInterface.session,
    input.dslLibraryPath
  )

  lazy val tagCreatorsAndTag: Try[List[TagCreator]] = KraptrEngine.buildTagCreators(dslContext)

  import com.hortonworks.dataplane.profilers.commons.Parsers._

  def dryRunSettingsAndPathIfItIsADryRun: Option[(DryRunSettings, String)] = {
    val arguments = Json.parse(args.head)
    val isADryRun = (arguments \ "is_dry_run").asOpt[Boolean].getOrElse(false)
    if (isADryRun) {
      val settingsFile = (arguments \ "dry_run_settings_file").as[String]
      val settingsFileHandler = HDFSFile(hdfsFileSystem, settingsFile)
      val settings = Json.parse(settingsFileHandler.readContent).as[DryRunSettings]
      hdfsFileSystem.delete(settingsFileHandler.filepath, false)
      val path = (arguments \ "dry_run_output_path").as[String]
      Some(settings, path)
    }
    else None
  }


  private def parseInput(json: String): InputArgs = {
    val input = ProfilerInput(json)

    import org.json4s._
    implicit val jsonDefaultFormats: DefaultFormats.type = DefaultFormats

    val sampleSize = input.getJobConfig("sampleSize").extract[String].toLong
    val configRoot = input.getJobConfig("configRoot").extract[String]
    val path = input.getClusterConfig("assetMetricsPath").extract[String]
    val dslLibraryPath = input.getJobConfig("dslLibraryPath").extractOpt[String]

    InputArgs(input, input.getAtlasInfo(), input.getHiveTables(),
      sampleSize, path, configRoot, dslLibraryPath)

  }
}
