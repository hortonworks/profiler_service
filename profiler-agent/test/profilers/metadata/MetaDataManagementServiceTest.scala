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

package profilers.metadata

import java.io.{File, PrintWriter}

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.scalatest.BeforeAndAfterAll
import org.scalatestplus.play.PlaySpec
import play.api.libs.Files
import play.api.libs.Files.TemporaryFile
import play.api.test.{DefaultAwaitTimeout, FutureAwaits}
import profilers.metadata.models.Requests._
import profilers.metadata.models.{Requests, _}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class MetaDataManagementServiceTest extends PlaySpec with BeforeAndAfterAll with FutureAwaits with DefaultAwaitTimeout {
  val resourcePath: String = getClass.getResource("/").getPath
  val file: File = new File(resourcePath, "metadatamanagementservicetest")
  val fileSystem: FileSystem = FileSystem.get(new Configuration())


  override def beforeAll(): Unit = {
    super.beforeAll()
    FileUtils.deleteDirectory(file)
    file.mkdir()
  }


  "MetaDataManagementService" must {

    "should not throw error when global directories doesn't exist" in {
      val metaRootPath: String = createDirectoryAndGetPath("test1")

      val schema: Map[String, ProfilerMetaConfiguration] = Map(
        "profiler1" -> ProfilerMetaConfiguration(metaRootPath, List(MetaDataManagementSchema(
          "profiler1", MetaType.global, "parentnode", MetaFileType.json, ".extension1", MetaFilePermission.create
        )))
      )
      val service = new MetaDataManagementService(fileSystem)
      val nodes = await(service.listNodes(schema)(ListNodes(MetaContext(None, "profiler1", "profiler1"), None)))
      assert(nodes.nodes.isEmpty)
    }

    "should not throw error when instance directories doesn't exist" in {
      val metaRootPath: String = createDirectoryAndGetPath("test2")

      val schema: Map[String, ProfilerMetaConfiguration] = Map(
        "profiler1" -> ProfilerMetaConfiguration(metaRootPath, List(MetaDataManagementSchema(
          "profiler1", MetaType.instance, "parentnode", MetaFileType.json, ".extension1", MetaFilePermission.create
        )))
      )
      val service = new MetaDataManagementService(fileSystem)
      val nodes = await(service.listNodes(schema)(ListNodes(MetaContext(Some("profilerinstance"), "profiler1", "profiler1"), None)))
      assert(nodes.nodes.isEmpty)
    }

    "should list all nodes for a meta configuration path for configuration of instance type" in {
      val metaRootPath: String = createDirectoryAndGetPath("test3")

      val schema: Map[String, ProfilerMetaConfiguration] = Map(
        "profiler1" -> ProfilerMetaConfiguration(metaRootPath, List(MetaDataManagementSchema(
          "profiler1", MetaType.instance, "parentnode", MetaFileType.json, ".extension1", MetaFilePermission.create
        )))
      )
      val service = new MetaDataManagementService(fileSystem)
      val create1 = service.createNode(schema)(CreateNode(MetaContext(Some("profilerinstance"), "profiler1", "profiler1"), "node1", "content1"))
      val create2 = service.createNode(schema)(CreateNode(MetaContext(Some("profilerinstance"), "profiler1", "profiler1"), "node2", "content2"))
      val nodes = await(create1.flatMap(_ => create2).flatMap(
        _ => service.listNodes(schema)(ListNodes(MetaContext(Some("profilerinstance"), "profiler1", "profiler1"), None))
      ))
      assert(nodes.nodes.toSet == Set("node1", "node2"))
    }


    "should list all nodes for a meta configuration path for configuration of global type" in {
      val metaRootPath: String = createDirectoryAndGetPath("test4")

      val schema: Map[String, ProfilerMetaConfiguration] = Map(
        "profiler1" -> ProfilerMetaConfiguration(metaRootPath, List(MetaDataManagementSchema(
          "profiler1", MetaType.global, "parentnode", MetaFileType.json, ".extension1", MetaFilePermission.create
        )))
      )
      val service = new MetaDataManagementService(fileSystem)
      val create1 = service.createNode(schema)(CreateNode(MetaContext(None, "profiler1", "profiler1"), "node1", "content1"))
      val create2 = service.createNode(schema)(CreateNode(MetaContext(None, "profiler1", "profiler1"), "node2", "content2"))
      val nodes = await(create1.flatMap(_ => create2).flatMap(
        _ => service.listNodes(schema)(ListNodes(MetaContext(None, "profiler1", "profiler1"), None))
      ))
      assert(nodes.nodes.toSet == Set("node1", "node2"))
    }


    "should be able write and read contents of a file for configuration of instance type" in {
      val metaRootPath: String = createDirectoryAndGetPath("test5")

      val schema: Map[String, ProfilerMetaConfiguration] = Map(
        "profiler1" -> ProfilerMetaConfiguration(metaRootPath, List(MetaDataManagementSchema(
          "profiler1", MetaType.instance, "parentnode", MetaFileType.json, ".extension1", MetaFilePermission.create
        )))
      )
      val service = new MetaDataManagementService(fileSystem)
      val create1 = service.createNode(schema)(CreateNode(MetaContext(Some("profilerinstance"), "profiler1", "profiler1"), "node1", "content1"))
      val create2 = service.createNode(schema)(CreateNode(MetaContext(Some("profilerinstance"), "profiler1", "profiler1"), "node2", "content2"))
      val content = await(create1.flatMap(_ => create2).flatMap(
        _ => service.readNodeContent(schema)(ReadNodeContent(MetaContext(Some("profilerinstance"), "profiler1", "profiler1"), "node1"))
      ))
      assert(content == "content1")
    }

    "should be able write and read contents of a file for configuration of global type" in {
      val metaRootPath: String = createDirectoryAndGetPath("test5")

      val schema: Map[String, ProfilerMetaConfiguration] = Map(
        "profiler1" -> ProfilerMetaConfiguration(metaRootPath, List(MetaDataManagementSchema(
          "profiler1", MetaType.global, "parentnode", MetaFileType.json, ".extension1", MetaFilePermission.create
        )))
      )
      val service = new MetaDataManagementService(fileSystem)
      val create1 = service.createNode(schema)(CreateNode(MetaContext(None, "profiler1", "profiler1"), "node1", "content1"))
      val create2 = service.createNode(schema)(CreateNode(MetaContext(None, "profiler1", "profiler1"), "node2", "content2"))
      val content = await(create1.flatMap(_ => create2).flatMap(
        _ => service.readNodeContent(schema)(ReadNodeContent(MetaContext(None, "profiler1", "profiler1"), "node1"))
      ))
      assert(content == "content1")
    }


    "should be able overwrite contents of a file for configuration of global type" in {
      val metaRootPath: String = createDirectoryAndGetPath("test6")

      val schema: Map[String, ProfilerMetaConfiguration] = Map(
        "profiler1" -> ProfilerMetaConfiguration(metaRootPath, List(MetaDataManagementSchema(
          "profiler1", MetaType.global, "parentnode", MetaFileType.json, ".extension1", MetaFilePermission.create
        )))
      )
      val service = new MetaDataManagementService(fileSystem)
      val create1: Future[Responses.CreateNodeResponse] = service.createNode(schema)(CreateNode(MetaContext(None, "profiler1", "profiler1"), "node1", "content1"))
      val create2: Future[Responses.CreateNodeResponse] = service.createNode(schema)(CreateNode(MetaContext(None, "profiler1", "profiler1"), "node2", "content2"))
      val writeContents = create1.flatMap(_ => create2)
      val content = await(writeContents.flatMap(_ =>
        service.overwriteNodeContent(schema)(OverwriteNodeContent(MetaContext(None, "profiler1", "profiler1"), "node2", "content5")))
        .flatMap(_ => service.readNodeContent(schema)(ReadNodeContent(MetaContext(None, "profiler1", "profiler1"), "node2"))))
      assert(content == "content5")
    }

    "should be able overwrite contents of a file for configuration of instance type" in {
      val metaRootPath: String = createDirectoryAndGetPath("test7")

      val schema: Map[String, ProfilerMetaConfiguration] = Map(
        "profiler1" -> ProfilerMetaConfiguration(metaRootPath, List(MetaDataManagementSchema(
          "profiler1", MetaType.instance, "parentnode", MetaFileType.json, ".extension1", MetaFilePermission.create
        )))
      )
      val service = new MetaDataManagementService(fileSystem)
      val create1: Future[Responses.CreateNodeResponse] = service.createNode(schema)(CreateNode(MetaContext(Some("profilerinstance"), "profiler1", "profiler1"), "node1", "content1"))
      val create2: Future[Responses.CreateNodeResponse] = service.createNode(schema)(CreateNode(MetaContext(Some("profilerinstance"), "profiler1", "profiler1"), "node2", "content2"))
      val writeContents = create1.flatMap(_ => create2)
      val content = await(writeContents.flatMap(_ =>
        service.overwriteNodeContent(schema)(OverwriteNodeContent(MetaContext(Some("profilerinstance"), "profiler1", "profiler1"), "node2", "content5")))
        .flatMap(_ => service.readNodeContent(schema)(ReadNodeContent(MetaContext(Some("profilerinstance"), "profiler1", "profiler1"), "node2"))))
      assert(content == "content5")
    }


    "should be able delete a file for configuration of instance type" in {
      val metaRootPath: String = createDirectoryAndGetPath("test8")

      val schema: Map[String, ProfilerMetaConfiguration] = Map(
        "profiler1" -> ProfilerMetaConfiguration(metaRootPath, List(MetaDataManagementSchema(
          "profiler1", MetaType.instance, "parentnode", MetaFileType.json, ".extension1", MetaFilePermission.create
        )))
      )
      val service = new MetaDataManagementService(fileSystem)
      val create1: Future[Responses.CreateNodeResponse] = service.createNode(schema)(CreateNode(MetaContext(Some("profilerinstance"), "profiler1", "profiler1"), "node1", "content1"))
      val create2: Future[Responses.CreateNodeResponse] = service.createNode(schema)(CreateNode(MetaContext(Some("profilerinstance"), "profiler1", "profiler1"), "node2", "content2"))
      val nodes = await(create1.flatMap(_ => create2).flatMap(
        _ => service.deleteNode(schema)(DeleteNode(MetaContext(Some("profilerinstance"), "profiler1", "profiler1"), "node1"))
      ).flatMap(_ =>
        service.listNodes(schema)(ListNodes(MetaContext(Some("profilerinstance"), "profiler1", "profiler1"), None))
      ))
      assert(nodes.nodes.toSet == Set("node2"))
    }

    "should be able delete a file for configuration of global type" in {
      val metaRootPath: String = createDirectoryAndGetPath("test8")

      val schema: Map[String, ProfilerMetaConfiguration] = Map(
        "profiler1" -> ProfilerMetaConfiguration(metaRootPath, List(MetaDataManagementSchema(
          "profiler1", MetaType.global, "parentnode", MetaFileType.json, ".extension1", MetaFilePermission.create
        )))
      )
      val service = new MetaDataManagementService(fileSystem)
      val create1: Future[Responses.CreateNodeResponse] = service.createNode(schema)(CreateNode(MetaContext(None, "profiler1", "profiler1"), "node1", "content1"))
      val create2: Future[Responses.CreateNodeResponse] = service.createNode(schema)(CreateNode(MetaContext(None, "profiler1", "profiler1"), "node2", "content2"))
      val nodes = await(create1.flatMap(_ => create2).flatMap(
        _ => service.deleteNode(schema)(DeleteNode(MetaContext(Some("profilerinstance"), "profiler1", "profiler1"), "node1"))
      ).flatMap(_ =>
        service.listNodes(schema)(ListNodes(MetaContext(None, "profiler1", "profiler1"), None))
      ))
      assert(nodes.nodes.toSet == Set("node2"))
    }


    "should be able upload a file for configuration of global type" in {
      val metaRootPath: String = createDirectoryAndGetPath("test9")
      val temporaryFile = new File(metaRootPath, "tempfile")
      val writer = new PrintWriter(temporaryFile)
      writer.write("content10")
      writer.close()

      val schema: Map[String, ProfilerMetaConfiguration] = Map(
        "profiler1" -> ProfilerMetaConfiguration(metaRootPath, List(MetaDataManagementSchema(
          "profiler1", MetaType.global, "parentnode", MetaFileType.json, ".extension1", MetaFilePermission.create
        )))
      )
      val service = new MetaDataManagementService(fileSystem)
      val create1: Future[Responses.CreateNodeResponse] = service.createNode(schema)(CreateNode(MetaContext(None, "profiler1", "profiler1"), "node1", "content1"))
      val create2: Future[Responses.CreateNodeResponse] = service.uploadNode(schema)(Requests.UploadNode(MetaContext(None, "profiler1", "profiler1"), "node2", TemporaryFile(temporaryFile)))
      val nodes = await(create1.flatMap(_ => create2).flatMap(
        _ => service.listNodes(schema)(ListNodes(MetaContext(None, "profiler1", "profiler1"), None))
      ))
      assert(nodes.nodes.toSet == Set("node1", "node2"))
    }

  }

  private def createDirectoryAndGetPath(testidentifier: String) = {
    val testsRoot = new File(file, testidentifier)
    testsRoot.mkdir()
    val metaRootPath = testsRoot.getAbsolutePath
    metaRootPath
  }

  override def afterAll(): Unit = {

    FileUtils.deleteDirectory(file)
    super.afterAll()
  }


}
