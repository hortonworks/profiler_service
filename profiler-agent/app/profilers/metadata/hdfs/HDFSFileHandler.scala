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

package profilers.metadata.hdfs

import java.nio.charset.Charset

import org.apache.hadoop.fs._
import play.api.libs.Files
import profilers.metadata.models.Responses.FileMetaData

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


case class HDFSFileHandler(fileSystem: FileSystem, filepath: Path) {

  def exists: Boolean = fileSystem.exists(filepath)

  def isDirectory: Boolean = fileSystem.isDirectory(filepath)

  def isFile: Boolean = fileSystem.isFile(filepath)

  def deleteFile: Boolean = fileSystem.delete(filepath, false)

  def listFiles: List[HDFSFileHandler] = {
    val fileIterator: RemoteIterator[LocatedFileStatus] = fileSystem.listFiles(filepath, false)
    fold(fileIterator)
  }

  @tailrec
  private def fold(iterator: RemoteIterator[LocatedFileStatus],
                   collectedPaths: List[HDFSFileHandler] = List.empty[HDFSFileHandler]): List[HDFSFileHandler] = {
    if (iterator.hasNext) {
      val nextFile = iterator.next()
      fold(iterator, collectedPaths :+ HDFSFileHandler(fileSystem, new Path(filepath, nextFile.getPath.getName)))
    }
    else {
      collectedPaths
    }
  }

  def readContent: String = {
    val inputStream: FSDataInputStream = fileSystem.open(filepath)
    scala.io.Source.fromInputStream(inputStream).mkString
  }


  def createChildFile(name: String, content: String): Future[HDFSFileHandler] = {
    val childPath = new Path(filepath, name)
    if (fileSystem.exists(childPath)) {
      Future.failed(new Exception(s"Cannot create file. path ${childPath.toString} already exist"))
    }
    else {
      Future {
        val stream: FSDataOutputStream = fileSystem.create(childPath)
        stream.write(content.getBytes(Charset.forName("UTF-8")))
        stream.close()
        HDFSFileHandler(fileSystem, childPath)
      }
    }
  }


  def createChildFileFromTemporaryFileAndDeleteTemporaryFile(name: String, localFile: Files.TemporaryFile): Future[HDFSFileHandler] = {
    val childPath = new Path(filepath, name)
    if (fileSystem.exists(childPath)) {
      Future.failed(new Exception(s"Cannot create file. path ${childPath.toString} already exist"))
    }
    else {
      Future {
        fileSystem.copyFromLocalFile(true, new Path(localFile.file.getAbsolutePath), childPath)
        HDFSFileHandler(fileSystem, childPath)
      }
    }
  }

  def createChildDirectoryIfNotExists(name: String): Future[HDFSFileHandler] = {
    val childPath = HDFSFileHandler(fileSystem, new Path(filepath, name))
    childPath.mkDirIfNotExists.map(_ => childPath)
  }

  def mkDirIfNotExists: Future[Unit] = {
    if (exists) {
      if (isFile) {
        Future.failed(new Exception(s"Failed to create directory, Path ${filepath.toString} already exists as a file"))
      }
      else {
        Future.successful(Unit)
      }
    }
    else {
      Future {
        fileSystem.mkdirs(filepath)
      }
    }
  }


  def getFileMetaData(): Future[FileMetaData] = {
    if (exists) {
      if (isFile) {
        val status = fileSystem.getFileStatus(filepath)
        val modificationTime = status.getModificationTime
        val sizeOfFile = status.getLen
        Future.successful(FileMetaData(path, modificationTime, sizeOfFile))
      }
      else {
        Future.failed(new Exception(s"File $path does exist.But it is a directory"))
      }
    }
    else {
      Future.failed(new Exception(s"File $path does not exist"))
    }
  }


  def createFileIfNotExist(content: String): Future[Unit] = {
    if (exists) {
      Future.failed(new Exception(s"File $path already exist .Cannot write data"))
    }
    else Future {
      val stream = fileSystem.create(filepath)
      stream.write(content.getBytes(Charset.forName("UTF-8")))
      stream.close()
    }
  }

  def name: String = filepath.getName

  def path: String = filepath.toString

  def readLines: List[String] = {
    if (isDirectory) {
      listFiles.flatMap(_.readLines)
    }
    else {
      scala.io.Source.fromInputStream(fileSystem.open(filepath)).getLines().toList
    }
  }
}
