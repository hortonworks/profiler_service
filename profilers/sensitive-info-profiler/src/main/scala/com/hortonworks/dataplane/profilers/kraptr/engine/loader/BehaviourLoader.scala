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
import com.hortonworks.dataplane.profilers.kraptr.models.behaviour.{BehaviourDefinition, Behaviours}
import com.hortonworks.dataplane.profilers.kraptr.vfs.VFSFile
import com.typesafe.scalalogging.LazyLogging
import play.api.libs.json.Json

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}


object BehaviourLoader extends LazyLogging {

  val behaviourPath: String = "behaviours"
  val behaviourExtension = ".kraptr_behaviour.json"

  def loadBehaviours(context: KraptrContext): Try[List[BehaviourDefinition]] = {
    val behaviourDirectoryPath: String =
      s"${context.configurationPath}${context.fileSystem.pathSeparator}" + s"${context.kraptrSettingsDirectory}${context.fileSystem.pathSeparator}" +
        behaviourPath
    logger.info(s"Trying to load behaviours from $behaviourDirectoryPath")
    val behaviourDirectory = context.fileSystem.getFile(behaviourDirectoryPath)

    behaviourDirectory.exists match {
      case true =>
        behaviourDirectory.isDirectory match {
          case true =>
            loadBehavioursWithExtensionFrom(behaviourDirectory)
          case false =>
            val errorMessage = s"path exist at ${context.kraptrSettingsDirectory}${context.fileSystem.pathSeparator}$behaviourPath.But it is a file." +
              s" No behaviours loaded"
            logger.error(errorMessage)
            Failure(new Exception(errorMessage))
        }

      case false =>
        val errorMessage = s"path doesn't exist at ${context.kraptrSettingsDirectory}${context.fileSystem.pathSeparator}$behaviourPath. No custom behaviours loaded"
        logger.error(errorMessage)
        Success(List.empty[BehaviourDefinition])
    }

  }


  private def loadBehavioursWithExtensionFrom(behaviourDirectory: VFSFile): Try[List[BehaviourDefinition]] = {
    val validFiles = behaviourDirectory.listFiles.filter(_.name.endsWith(behaviourExtension))
    buildBehaviours(validFiles)
  }

  @tailrec
  private def buildBehaviours(files: Seq[VFSFile],
                              behaviours: List[BehaviourDefinition] = List.empty[BehaviourDefinition]):
  Try[List[BehaviourDefinition]] = {
    files.isEmpty match {
      case true => Success(behaviours)
      case false =>
        loadBehaviourFromFile(files.head) match {
          case Failure(error) =>
            logger.error(s"Failed to load behaviour definition file ${files.head.name}", error)
            Failure(error)
          case Success(newBehaviours) =>
            logger.info(s"Loaded behaviour definitions from file ${files.head.name}")
            buildBehaviours(files.tail, behaviours ++ newBehaviours)
        }
    }
  }


  private def loadBehaviourFromFile(file: VFSFile): Try[List[BehaviourDefinition]] = {
    import com.hortonworks.dataplane.profilers.kraptr.models.behaviour.BehaviourParsers._
    file.isFile match {
      case true =>
        Try({
          val behaviours = Json.parse(file.readContent).as[Behaviours]
          logger.info(s"loaded behaviour group ${behaviours.groupName}")
          behaviours.behaviours
        })
      case false =>
        logger.error(s"${file.name} is a directory. Will not be loaded")
        Success(List.empty)
    }
  }

}
