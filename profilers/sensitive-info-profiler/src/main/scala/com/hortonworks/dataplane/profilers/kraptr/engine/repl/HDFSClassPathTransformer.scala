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

package com.hortonworks.dataplane.profilers.kraptr.engine.repl

import java.io.File

import com.hortonworks.dataplane.profilers.kraptr.vfs.hdfs.HDFSFileSystem
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.annotation.tailrec
import scala.util.Try

class HDFSClassPathTransformer(fileSystem: FileSystem) extends LazyLogging {

  private lazy val tempDirectory = new File(System.getProperty("java.io.tmpdir"))
  private lazy val classPathDirectory: File = {
    val classPathDirectory = new File(tempDirectory, "tempLocalClassPathDir")
    if (classPathDirectory.exists()) {
      logger.warn(s"classpath directory ${classPathDirectory.getAbsoluteFile}  exists " +
        s". deleting ")
      classPathDirectory.delete()
    }
    logger.warn(s"created classpath directory ${classPathDirectory.getAbsoluteFile}")
    classPathDirectory.mkdir()
    classPathDirectory
  }


  def processHDFSDependenciesInClasspath(classPathString: String): Try[List[String]] = {
    Try(searchIn(classPathString))
  }

  def searchInHDFSLibraryPath(dslLibraryPath: String): Try[List[String]] = Try {
    val system = HDFSFileSystem(fileSystem)
    val dslLibrary = system.getFile(dslLibraryPath)
    if (dslLibrary.isFile && dslLibrary.name.endsWith(".jar")) {
      List(copyToLocal(dslLibrary.path))
    }
    else {
      val jarsInHDFS = dslLibrary.listFiles.filter(x => x.isFile && x.name.endsWith(".jar"))
      jarsInHDFS.map(x => copyToLocal(x.path))
    }
  }

  private def copyToLocal(hdfsPath: String): String = {
    val path = new Path(hdfsPath)
    val localPathFile = new File(classPathDirectory, path.getName)
    val localPath = new Path(localPathFile.getAbsolutePath)
    fileSystem.copyToLocalFile(path, localPath)
    logger.info(s"moved $hdfsPath to ${localPathFile.getAbsolutePath}")
    localPathFile.getAbsolutePath
  }


  @tailrec private def searchIn(classPathString: String, foundOutPaths: List[String]
  = List.empty[String]): List[String] = {
    val hdfsPrefix = "hdfs:///"
    val indexOfHdfsPathStart: Int = classPathString.indexOf(hdfsPrefix)
    if (indexOfHdfsPathStart == -1) {
      foundOutPaths
    }
    else {
      val startingAtHdfs = classPathString.substring(indexOfHdfsPathStart)
      val indexOfColon: Int = startingAtHdfs.substring(hdfsPrefix.length).indexOf(":")
      if (indexOfColon == -1) {
        logger.info(s"Found hdfs path in classpath $startingAtHdfs")
        foundOutPaths :+ copyToLocal(startingAtHdfs)
      }
      else {
        val updatedIndex = indexOfColon + hdfsPrefix.length
        val pathFound = startingAtHdfs.substring(0, updatedIndex)
        logger.info(s"Found hdfs path in classpath $pathFound")
        val newFoundOutPaths = foundOutPaths :+ copyToLocal(pathFound)
        searchIn(startingAtHdfs.substring(updatedIndex), newFoundOutPaths)
      }
    }
  }

}
