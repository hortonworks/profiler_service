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

import org.apache.hadoop.fs.{FileSystem, Path}
import play.api.Logger
import profilers.metadata.errorhandling.{InvalidRequestException, NodeAlreadyExistsException, NodeNotFoundException, NotAllowedException}
import profilers.metadata.hdfs.HDFSFileHandler
import profilers.metadata.models.MetaFilePermission.MetaFilePermission
import profilers.metadata.models.Requests._
import profilers.metadata.models.Responses.{CreateNodeResponse, GetNodesInfoResponse, NodeInfo, Nodes}
import profilers.metadata.models.{MetaDataManagementSchema, MetaFilePermission, MetaType, ProfilerMetaConfiguration}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class MetaDataManagementService(fileSystem: FileSystem) {

  val ascendingPermissionIndexes: Map[MetaFilePermission, Int] =
    Map(MetaFilePermission.read -> 0, MetaFilePermission.write -> 1, MetaFilePermission.create -> 2)

  private case class FileHandlerAndSchemaOfNode(handler: HDFSFileHandler, schema: MetaDataManagementSchema)

  def listNodes(profilerSchemas: Map[String, ProfilerMetaConfiguration])(request: ListNodes): Future[Nodes] = {
    val eventualNodesWithoutExtension: Future[List[String]] = validateContextAndGetDirectory(profilerSchemas, request.context, MetaFilePermission.read)
      .map(
        nodeHandler => {
          Logger.info(s"Listing contents of directory ${nodeHandler.handler.path}")
          val fileNames = nodeHandler.handler.listFiles.map(x => x.name.trim)
            .filter(_ endsWith nodeHandler.schema.extension).map(_.dropRight(nodeHandler.schema.extension.length))
          request.startsWith.map(x => fileNames.filter(_ startsWith x)).getOrElse(fileNames)
        }
      )
    eventualNodesWithoutExtension.map(
      Nodes(request.context.id, _)
    )
  }

  def readNodeContent(profilerSchemas: Map[String, ProfilerMetaConfiguration])(request: ReadNodeContent): Future[String] = {
    validateContextAndGetDirectory(profilerSchemas, request.context, MetaFilePermission.read).flatMap(
      nodeHandler => {
        val allChildren: List[HDFSFileHandler] = nodeHandler.handler
          .listFiles
        val existingNodes: List[String] = getExistingNodes(nodeHandler, allChildren)
        if (existingNodes.contains(request.nodeName)) {
          val fileName: String = request.nodeName + nodeHandler.schema.extension
          val childFile = allChildren.filter(_.name == fileName).head
          Future(childFile.readContent).map(content => {
            Logger.info(s"read contents of ${childFile.path}")
            content
          })
        }
        else {
          Future.failed(NodeNotFoundException(s"node ${request.nodeName}  with extension ${nodeHandler.schema.extension} " +
            s"doesn't exist at path ${nodeHandler.handler.path}"))
        }
      }
    )
  }


  private def getExistingNodes(nodeHandler: FileHandlerAndSchemaOfNode, allChildren: List[HDFSFileHandler]) = {
    allChildren.map(x => x.name.trim)
      .filter(_ endsWith nodeHandler.schema.extension)
      .map(_.dropRight(nodeHandler.schema.extension.length))
  }

  def overwriteNodeContent(profilerSchemas: Map[String, ProfilerMetaConfiguration])(request: OverwriteNodeContent): Future[CreateNodeResponse] = {
    validateContextAndGetDirectory(profilerSchemas, request.context, MetaFilePermission.write).flatMap(
      nodeHandler => {
        val allChildren: List[HDFSFileHandler] = nodeHandler.handler
          .listFiles
        val existingNodes: List[String] = getExistingNodes(nodeHandler, allChildren)
        if (existingNodes.contains(request.nodeName)) {
          Future {
            val fileName: String = request.nodeName + nodeHandler.schema.extension
            val childFile: HDFSFileHandler = allChildren.filter(_.name == fileName).head
            childFile.deleteFile
            childFile
          } flatMap (childFile => {
            childFile.createFileIfNotExist(request.content).map(_ => {
              Logger.info(s"Overwritten contents of file ${childFile.path}")
              CreateNodeResponse(childFile.path)
            }
            )
          })
        }
        else {
          Future.failed(NodeNotFoundException(s"node ${request.nodeName}  with extension ${nodeHandler.schema.extension} " +
            s"doesn't exist at path ${nodeHandler.handler.path}. Cannot overwrite"))
        }
      }
    )
  }


  def createNode(profilerSchemas: Map[String, ProfilerMetaConfiguration])(request: CreateNode): Future[CreateNodeResponse] = {
    validateContextAndGetDirectory(profilerSchemas, request.context, MetaFilePermission.create).flatMap(
      nodeHandler => {
        val allChildren: List[HDFSFileHandler] = nodeHandler.handler
          .listFiles
        val existingNodes: List[String] = getExistingNodes(nodeHandler, allChildren)
        if (existingNodes.contains(request.nodeName)) {
          Future.failed(NodeAlreadyExistsException(s"node ${request.nodeName}  with extension ${nodeHandler.schema.extension} " +
            s"exist at path ${nodeHandler.handler.path}. Cannot create"))
        }
        else {
          val fileName: String = request.nodeName + nodeHandler.schema.extension
          nodeHandler.handler.createChildFile(fileName, request.content).map(
            childFile => {
              Logger.info(s"Created file ${childFile.path}")
              CreateNodeResponse(childFile.path)
            }
          )
        }
      }
    )
  }

  def uploadNode(profilerSchemas: Map[String, ProfilerMetaConfiguration])(request: UploadNode): Future[CreateNodeResponse] = {
    validateContextAndGetDirectory(profilerSchemas, request.context, MetaFilePermission.create).flatMap(
      nodeHandler => {
        val allChildren: List[HDFSFileHandler] = nodeHandler.handler
          .listFiles
        val existingNodes: List[String] = getExistingNodes(nodeHandler, allChildren)
        if (existingNodes.contains(request.nodeName)) {
          Future.failed(NodeAlreadyExistsException(s"node ${request.nodeName}  with extension ${nodeHandler.schema.extension} " +
            s"exist at path ${nodeHandler.handler.path}. Cannot create"))
        }
        else {
          val fileName: String = request.nodeName + nodeHandler.schema.extension
          nodeHandler.handler.createChildFileFromTemporaryFileAndDeleteTemporaryFile(fileName, request.temporaryFile).map(
            childFile => {
              Logger.info(s"Created file ${childFile.path}")
              CreateNodeResponse(childFile.path)
            }
          )
        }
      }
    )
  }


  def deleteNode(profilerSchemas: Map[String, ProfilerMetaConfiguration])(request: DeleteNode): Future[Unit] = {
    validateContextAndGetDirectory(profilerSchemas, request.context, MetaFilePermission.create).flatMap(
      nodeHandler => {
        val allChildren: List[HDFSFileHandler] = nodeHandler.handler
          .listFiles
        val existingNodes: List[String] = getExistingNodes(nodeHandler, allChildren)
        if (existingNodes.contains(request.nodeName)) {
          val fileName: String = request.nodeName + nodeHandler.schema.extension
          val childFile: HDFSFileHandler = allChildren.filter(_.name == fileName).head
          Future {
            childFile.deleteFile
            Logger.info(s"deleted file ${childFile.path}")
          }
        }
        else {
          Future.failed(NodeNotFoundException(s"node ${request.nodeName}  with extension ${nodeHandler.schema.extension} " +
            s"doesn't exist at path ${nodeHandler.handler.path}"))
        }
      }
    )
  }


  def getNodeInfo(profilerSchemas: Map[String, ProfilerMetaConfiguration])(getInformationAboutNodes: GetInformationAboutNodes): Future[GetNodesInfoResponse] = {
    val allDistinctContexts = getInformationAboutNodes.nodes.map(_.context).distinct
    val possibleFileHandlerAndSchemas: Future[List[(MetaContext, FileHandlerAndSchemaOfNode)]] =
      Future.sequence(allDistinctContexts.map(x => validateContextAndGetDirectory(profilerSchemas, x, MetaFilePermission.read).map(x -> _)))
    val eventualNodeInformation: Future[List[NodeInfo]] = possibleFileHandlerAndSchemas.flatMap(
      fileHandlerAndSchemas => {
        val fileHandlerAndSchemasMap = fileHandlerAndSchemas.toMap
        val eventualNodeInfo: List[Future[NodeInfo]] = getInformationAboutNodes.nodes.map(
          node => {
            val handlerAndSchemaOfNode = fileHandlerAndSchemasMap(node.context)
            val allChildren: List[HDFSFileHandler] = handlerAndSchemaOfNode.handler
              .listFiles
            val existingNodes: List[String] = getExistingNodes(handlerAndSchemaOfNode, allChildren)
            if (existingNodes.contains(node.nodeName)) {
              val fileName: String = node.nodeName + handlerAndSchemaOfNode.schema.extension
              val childFile: HDFSFileHandler = allChildren.filter(_.name == fileName).head
              childFile.getFileMetaData().map(
                x => NodeInfo(node.id, node.nodeName, node.context, isExists = true, Some(x))
              )
            }
            else {
              Future.successful(NodeInfo(node.id, node.nodeName, node.context, isExists = false, None))
            }
          }
        )
        Future.sequence(eventualNodeInfo)
      }
    )
    eventualNodeInformation.map(
      GetNodesInfoResponse
    )
  }


  private def isPermissionProvided(allowedPermission: MetaFilePermission
                                   , askingPermission: MetaFilePermission): Boolean = {
    ascendingPermissionIndexes(allowedPermission) >= ascendingPermissionIndexes(askingPermission)
  }

  private def validateContextAndGetDirectory(profilerSchemas: Map[String, ProfilerMetaConfiguration],
                                             context: MetaContext,
                                             requiredPermission: MetaFilePermission): Future[FileHandlerAndSchemaOfNode] = {
    profilerSchemas.get(context.profilerId).map(
      metaConfiguration => {
        val applicableSchema: List[MetaDataManagementSchema] = metaConfiguration.metaConfigs.filter(_.id == context.id)
        if (applicableSchema.isEmpty) {
          Future.failed(InvalidRequestException(s"No id ${context.id} exist for profiler ${context.profilerId}"))
        }
        else if (applicableSchema.size != 1) {
          Future.failed(InvalidRequestException(s"Multiple definition for  id ${context.id} " +
            s"exist for profiler ${context.profilerId}"))
        }
        else {
          val schema: MetaDataManagementSchema = applicableSchema.head
          if (isPermissionProvided(schema.permission, requiredPermission)) {
            schema.metaType match {
              case MetaType.global =>
                val rootPathHandler: HDFSFileHandler = HDFSFileHandler(fileSystem, new Path(metaConfiguration.rootPath))
                rootPathHandler.mkDirIfNotExists.flatMap(_ => rootPathHandler.createChildDirectoryIfNotExists(schema.nodeName))
                  .map(FileHandlerAndSchemaOfNode(_, schema))
              case MetaType.instance =>
                context.instance.map(
                  instance => {
                    val rootPathHandler: HDFSFileHandler = HDFSFileHandler(fileSystem, new Path(metaConfiguration.rootPath))
                    val metaConfigNodeHandler: Future[HDFSFileHandler] = rootPathHandler.mkDirIfNotExists
                      .flatMap(_ => rootPathHandler.createChildDirectoryIfNotExists(schema.nodeName))
                    metaConfigNodeHandler.flatMap(metaConfigRoot =>
                      metaConfigRoot.createChildDirectoryIfNotExists(instance)
                    )
                      .map(FileHandlerAndSchemaOfNode(_, schema))
                  }
                ).getOrElse(
                  Future.failed(InvalidRequestException(s"Missing mandatory parameter instance in context $context"))
                )
              case unknown =>
                Future.failed(InvalidRequestException(s"Unnecessary meta type $unknown"))
            }
          } else {
            Future.failed(NotAllowedException(s"Not allowed allowedPermission : ${schema.permission.toString}  " +
              s"askingPermission : $requiredPermission"))
          }
        }
      }
    ).getOrElse(Future.failed(InvalidRequestException(s"No schema exist for profiler ${context.profilerId}")))
  }

}
