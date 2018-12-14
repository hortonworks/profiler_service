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

package com.hortonworks.dataplane.profilers.kraptr.engine.loader

import com.hortonworks.dataplane.profilers.kraptr.common.KraptrContext
import com.hortonworks.dataplane.profilers.kraptr.models.dsl.{DSLRulesForAProfilerInstance, DSLandTags}
import com.hortonworks.dataplane.profilers.kraptr.vfs.VFSFile
import com.typesafe.scalalogging.LazyLogging
import play.api.libs.json.Json

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

object DSLLoader extends LazyLogging {
  val dslPath: String = "dsl"
  val dslExtension = ".kraptr_dsl.json"

  def loadDSLs(context: KraptrContext): Try[List[DSLandTags]] = {
    val dslDirectoryPath: String =
      s"${context.configurationPath}${context.fileSystem.pathSeparator}" + s"${context.kraptrSettingsDirectory}${context.fileSystem.pathSeparator}" +
        s"$dslPath${context.fileSystem.pathSeparator}${context.profilerInstance}"
    logger.info(s"Trying to load dsls from $dslDirectoryPath")
    val dslDirectory = context.fileSystem.getFile(dslDirectoryPath)

    dslDirectory.exists match {
      case true =>
        dslDirectory.isDirectory match {
          case true =>
            loadDSLSWithExtensionFrom(dslDirectory)
          case false =>
            val errorMessage = s"path exist at ${context.kraptrSettingsDirectory}${context.fileSystem.pathSeparator}$dslPath.But it is a file." +
              s" No behaviours loaded"
            logger.error(errorMessage)
            Failure(new Exception(errorMessage))
        }

      case false =>
        val errorMessage = s"path doesn't exist at ${context.kraptrSettingsDirectory}${context.fileSystem.pathSeparator}$dslPath. No dsls loaded"
        logger.error(errorMessage)
        Success(List.empty[DSLandTags])
    }

  }


  private def loadDSLSWithExtensionFrom(tagDirectory: VFSFile): Try[List[DSLandTags]] = {
    val validFiles = tagDirectory.listFiles.filter(_.name.endsWith(dslExtension))
    buildDSLs(validFiles)
  }

  @tailrec
  private def buildDSLs(dsls: Seq[VFSFile],
                        tags: List[DSLandTags] = List.empty[DSLandTags]):
  Try[List[DSLandTags]] = {
    dsls.isEmpty match {
      case true => Success(tags)
      case false =>
        loadDSLSFromFile(dsls.head) match {
          case Failure(error) =>
            logger.error(s"Failed to load dsls definition file ${dsls.head.name}", error)
            Failure(error)
          case Success(newTags) =>
            logger.info(s"Loaded dsls definitions from file ${dsls.head.name}")
            buildDSLs(dsls.tail, tags ++ newTags)
        }
    }
  }


  private def loadDSLSFromFile(file: VFSFile): Try[List[DSLandTags]] = {
    import com.hortonworks.dataplane.profilers.kraptr.models.dsl.DSLandTagParsers._
    file.isFile match {
      case true =>
        Try({
          val value = Json.parse(file.readContent)
          val dslRules = value.as[DSLRulesForAProfilerInstance]
          logger.info(s"loaded dslRule group ${dslRules.groupName}")
          dslRules.dsls.filter(_.isEnabled)
        }).flatMap(dsls => {
          val invalidDSLs = dsls.filter(!_.isValid)
          if (invalidDSLs.nonEmpty) {
            Failure(new Exception(s"Invalid dsls found ${invalidDSLs.mkString(",")}"))
          }
          else {
            Success(dsls)
          }
        })
      case false =>
        logger.error(s"${file.name} is a directory. Will not be loaded")
        Success(List.empty)
    }
  }
}
