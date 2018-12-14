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

package com.hortonworks.dataplane.profilers.kraptr.vfs.hdfs


import com.hortonworks.dataplane.profilers.kraptr.vfs.VFSFile
import org.apache.hadoop.fs._

import scala.annotation.tailrec


case class HDFSFile(fileSystem: FileSystem, location: String) extends VFSFile {

  val filepath: Path = new Path(location)

  override def exists: Boolean = fileSystem.exists(filepath)

  override def isDirectory: Boolean = fileSystem.isDirectory(filepath)

  override def isFile: Boolean = fileSystem.isFile(filepath)

  override def listFiles: List[VFSFile] = {
    val fileIterator: RemoteIterator[LocatedFileStatus] = fileSystem.listFiles(filepath, false)
    fold(fileIterator)
  }

  @tailrec
  private def fold(iterator: RemoteIterator[LocatedFileStatus],
                   collectedPaths: List[HDFSFile] = List.empty[HDFSFile]): List[HDFSFile] = {
    if (iterator.hasNext) {
      val nextFile = iterator.next()
      fold(iterator, collectedPaths :+ HDFSFile(fileSystem, nextFile.getPath.toString))
    }
    else {
      collectedPaths
    }
  }

  override def readContent: String = {
    val inputStream: FSDataInputStream = fileSystem.open(filepath)
    scala.io.Source.fromInputStream(inputStream).mkString
  }


  override def name: String = filepath.getName

  override def path: String = location

  override def readLines: List[String] = {
    if (isDirectory) {
      listFiles.flatMap(_.readLines)
    }
    else {
      scala.io.Source.fromInputStream(fileSystem.open(filepath)).getLines().toList
    }
  }
}
