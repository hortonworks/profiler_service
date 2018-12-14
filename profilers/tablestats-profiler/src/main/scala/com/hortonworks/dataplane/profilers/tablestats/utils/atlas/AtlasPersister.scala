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
package com.hortonworks.dataplane.profilers.tablestats.utils.atlas

import com.hortonworks.dataplane.profilers.commons.parse.AtlasInfo
import com.hortonworks.dataplane.profilers.commons.atlas.{AtlasConstants, AtlasInterface}
import com.hortonworks.dataplane.profilers.tablestats.utils.TableMetrics
import com.typesafe.scalalogging.LazyLogging
import org.apache.atlas.typesystem.json.InstanceSerialization
import org.apache.atlas.typesystem.{Referenceable, Struct}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scalaj.http.HttpResponse

case class AtlasRestCall(urlList:List[String], headers:List[(String, String)], params:List[(String, String)],
                         method:String = "GET", requestJson:String = "")

case class NamesAndQualifiedNames(dbName:String, clusterName:String, tableName:String, dbQualifiedName:String,
                                  tableQualifiedName:String, sdQualifiedName:String)

object AtlasPersister extends LazyLogging {
  def persistToAtlas(atlasInfo: AtlasInfo, dbName: String, tableName: String,
                     collectedMetrics: Map[String, Map[String, Any]],
                     tableMetrics: TableMetrics):Future[Unit] = {

    val atlasUser = atlasInfo.user
    val atlasURL = atlasInfo.url
    val atlasPassword = atlasInfo.password
    val clusterName = atlasInfo.clusterName
    val atlasUrlList = atlasURL.split(",").toList

    val atlasRestRequestHeaders = ("Authorization", AtlasConstants.getEncodedAuthorizationHeader(atlasUser,
      atlasPassword)) :: AtlasConstants.AtlasPostRequestHeaders

    val atlasPostCallObject = AtlasRestCall(atlasUrlList, atlasRestRequestHeaders, List(), "POST")

    val dbQualifiedName = AtlasConstants.getDbQualifiedName(dbName, clusterName)
    val tableQualifiedName = AtlasConstants.getTableQualifiedName(tableName, dbName, clusterName)
    val tableSdQualifiedName = tableQualifiedName + "_storage"

    val namesAndQualifiedNames = NamesAndQualifiedNames(dbName, clusterName, tableName, dbQualifiedName,
      tableQualifiedName, tableSdQualifiedName)

    val hiveTableReferenceable = new Referenceable(AtlasConstants.HiveTableTypeName)
    hiveTableReferenceable.set(AtlasConstants.QualifiedName, tableQualifiedName)
    val tableProfileData = new Struct(AtlasConstants.HiveTableProfileData)
    tableMetrics.rowCount.map(tableProfileData.set("rowCount", _))
    tableProfileData.set("samplePercent", tableMetrics.samplePercent)
    tableProfileData.set("sampleTime", tableMetrics.sampleTime)
    hiveTableReferenceable.set(AtlasConstants.ProfileDataAttr, tableProfileData)

    val tableGetParams = List((AtlasConstants.TypeAttribute, AtlasConstants.HiveTableTypeName),
      ("property", AtlasConstants.QualifiedName), ("value", tableQualifiedName))
    val getTableResponse = AtlasInterface.atlasApiHandler(atlasUrlList, AtlasConstants.atlasEntityRESTEndpoint,
      atlasRestRequestHeaders, tableGetParams)

    val tablePartialUpdateParams = List((AtlasConstants.TypeAttribute, AtlasConstants.HiveTableTypeName),
      ("property", AtlasConstants.QualifiedName), ("value", tableQualifiedName))

    val tablePartialUpdatePostCallObject = atlasPostCallObject.copy(params = tablePartialUpdateParams,
      requestJson = InstanceSerialization._toJson(hiveTableReferenceable))

    EntityPartialUpdateProcessor.updateEntityIfExistsInAtlas(tableQualifiedName, tablePartialUpdatePostCallObject,
      getTableResponse)

    val columnEntityList = collectedMetrics map {
      entry => {
        val columnName = entry._1
        val columnMetrics = entry._2
        val columnQualifiedName = AtlasConstants.getColumnQualifiedName(columnName, tableName, dbName, clusterName)
        val hiveColumnReferenceable = ColumnEntityConstructor.getColumnReferenceable(columnName, columnQualifiedName,
          columnMetrics)
        val columnReferenceableJSON = InstanceSerialization._toJson(hiveColumnReferenceable)
        logger.info("Column referenceable JSON => {}", columnReferenceableJSON)

        val columnGetParams = List((AtlasConstants.TypeAttribute, AtlasConstants.HiveColumnTypeName),
          ("property", AtlasConstants.QualifiedName), ("value", columnQualifiedName))

        val columnPartialUpdatePostCallObject = atlasPostCallObject.copy(params = columnGetParams,
          requestJson = columnReferenceableJSON)

        val getColumnResponse = AtlasInterface.atlasApiHandler(atlasUrlList, AtlasConstants.atlasEntityRESTEndpoint,
          atlasRestRequestHeaders, columnGetParams)
        getColumnResponse.flatMap(response => {
          logger.info("Column {} Response code {}, response content {}", columnName, response.code.toString, response.body)
          Future.successful()
        })

        EntityPartialUpdateProcessor.updateEntityIfExistsInAtlas(columnQualifiedName, columnPartialUpdatePostCallObject,
          getColumnResponse)

        hiveColumnReferenceable
      }
    }

    registerTableWithProfileDataIfNotExists(columnEntityList.toList, hiveTableReferenceable, atlasPostCallObject,
      namesAndQualifiedNames, getTableResponse)
  }

  def registerTableWithProfileDataIfNotExists(columnList:List[Referenceable], hiveTableReferenceable:Referenceable,
                                   postCallObj:AtlasRestCall, namesAndQualifiedNames: NamesAndQualifiedNames,
                                   getTableResponse:Future[HttpResponse[String]]):Future[Unit] = {
    getTableResponse flatMap {
      tableResponse => tableResponse.code match {
        case 404 => {
          import collection.JavaConverters._
          val hiveDbEntity = new Referenceable(AtlasConstants.HiveDbTypeName)
          hiveDbEntity.set(AtlasConstants.Name, namesAndQualifiedNames.dbName)
          hiveDbEntity.set(AtlasConstants.QualifiedName, namesAndQualifiedNames.dbQualifiedName)
          hiveDbEntity.set(AtlasConstants.ClusterName, namesAndQualifiedNames.clusterName)
          logger.debug("Constructed DB entity => {}", hiveDbEntity.toString)

          hiveTableReferenceable.set(AtlasConstants.DbAttribute, hiveDbEntity.getId)
          hiveTableReferenceable.set(AtlasConstants.Name, namesAndQualifiedNames.tableName)
          val tableSdEntity = new Referenceable(AtlasConstants.HiveSdTypeName)
          tableSdEntity.set(AtlasConstants.Name, namesAndQualifiedNames.sdQualifiedName)
          tableSdEntity.set(AtlasConstants.QualifiedName, namesAndQualifiedNames.sdQualifiedName)
          tableSdEntity.set("compressed", "false")
          tableSdEntity.set(AtlasConstants.TableAttribute, hiveTableReferenceable.getId)
          hiveTableReferenceable.set(AtlasConstants.TableSdAttribute, tableSdEntity)

          hiveTableReferenceable.set(AtlasConstants.ColumnsAtttribute, columnList.asJava)
          hiveTableReferenceable.set(AtlasConstants.DbAttribute, hiveDbEntity.getId)
          logger.debug("Constructed table entity => {}", hiveTableReferenceable.toString)

          logger.info("Attempting to serialize entities for table {}", namesAndQualifiedNames.tableQualifiedName)
          val requestJSON = InstanceSerialization._toJson(List(hiveDbEntity, hiveTableReferenceable).asJava)
          logger.info("Serialized JSON for request => {}", requestJSON)

          val tablePostResponse = AtlasInterface.atlasApiHandler(postCallObj.urlList,
            AtlasConstants.atlasEntityRESTEndpoint, postCallObj.headers, postCallObj.params,
            postCallObj.method, requestJSON)

          val tablePostResult = tablePostResponse flatMap {
            postResponse => postResponse.code match {
              case 201 | 200 => {
                logger.info("Create request successful with response code {} and body {}", postResponse.code.toString,
                  postResponse.body)
                Future.successful()
              }
              case _ => {
                logger.info("Create request failed with response code {} and body {}", postResponse.code.toString,
                  postResponse.body)
                Future.failed(new Exception("Create request failed"))
              }
            }
          }
          tablePostResult
        }
        case _ => {
          logger.info("Table {} already exists with updated profile data. Nothing to do",
            namesAndQualifiedNames.tableQualifiedName)
          Future.successful()
        }
      }
    }
  }
}

