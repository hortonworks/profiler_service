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

import com.typesafe.scalalogging.LazyLogging

import org.apache.atlas.typesystem.{Referenceable, Struct}
import org.apache.atlas.typesystem.json.InstanceSerialization

import scalaj.http.{Http, HttpResponse}

object AtlasPersister extends LazyLogging{
  def persistToAtlas(atlasURL: String, atlasRestRequestParams: List[(String, String)],clusterName: String,
                     dbName: String, tableName: String,
                     collectedMetrics: Map[String, Map[String, Any]],
                     tableRowCount:Long, sampleTime:String, samplePercent:String) {


    val entityURL = atlasURL + Constants.atlasEntityRESTEndpoint
    val partialUpdatePostURL = atlasURL + Constants.atlasEntityPartialUpdateRESTEndpoint
    logger.debug("Attempting to persist metrics to Atlas")

    val dbQualifiedName = dbName + "@" + clusterName
    val tableQualifiedName = dbName + "." + tableName + "@" + clusterName
    val tableSdQualifiedName = tableQualifiedName + "_storage"

    logger.debug("DB => {}, Table => {}, SD => {}", dbQualifiedName, tableQualifiedName, tableSdQualifiedName)

    val hiveDbEntity = new Referenceable("hive_db")
    hiveDbEntity.set(Constants.Name, dbName)
    hiveDbEntity.set(Constants.QualifiedName, dbQualifiedName)
    hiveDbEntity.set(Constants.ClusterName, clusterName)
    logger.debug("Constructed DB entity => {}", hiveDbEntity.toString)

    val hiveTableReferenceable = new Referenceable(Constants.HiveTableTypeName)
    hiveTableReferenceable.set(Constants.QualifiedName, tableQualifiedName)
    val tableProfileData = new Struct(Constants.HiveTableProfileData)
    tableProfileData.set("rowCount", tableRowCount)
    tableProfileData.set("samplePercent", samplePercent)
    tableProfileData.set("sampleTime", sampleTime)
    hiveTableReferenceable.set(Constants.ProfileDataAttr, tableProfileData)

    val tableGetParams = List((Constants.TypeAttribute, Constants.HiveTableTypeName), ("property", "qualifiedName"), ("value", tableQualifiedName))
    val getTableResponse = AtlasInterface.getData(entityURL, atlasRestRequestParams, tableGetParams)
    getTableResponse.code match {
      case 404 => {
        logger.debug("Table with qualifiedName {} not found in Atlas. Constructing table entity from scratch",
          tableQualifiedName)
        hiveTableReferenceable.set(Constants.DbAttribute, hiveDbEntity.getId)
        hiveTableReferenceable.set(Constants.Name, tableName)
        val tableSdEntity = new Referenceable(Constants.HiveSdTypeName)
        tableSdEntity.set(Constants.Name, tableSdQualifiedName)
        tableSdEntity.set(Constants.QualifiedName, tableSdQualifiedName)
        tableSdEntity.set("compressed", "false")
        tableSdEntity.set(Constants.TableAttribute, hiveTableReferenceable.getId)
        hiveTableReferenceable.set(Constants.TableSdAttribute, tableSdEntity)
      }
      case 200 => {
        logger.debug("Table with qualifiedName {} already exists in Atlas. Only the table profile data will be updated",
          tableQualifiedName)

        val tableJSON = InstanceSerialization._toJson(hiveTableReferenceable)
        val response = AtlasInterface.postDataPartialUpdate(partialUpdatePostURL, atlasRestRequestParams,
          tableJSON, Constants.HiveTableTypeName, Constants.QualifiedName, tableQualifiedName)
        response.code match {
          case 200 => {
            logger.info("Profile data update for table {} successful with response {}", tableQualifiedName, response.body)
          }
          case _ => {
            logger.warn("Profile data update for table {} failed with response {}", tableQualifiedName, response.body)
          }
        }
      }
      case _ => {
        logger.warn("Unexpected error code {}. Trying other approaches to persist the data", getTableResponse.code.toString)
      }
    }

    val column_entities = collectedMetrics map {
      entry => {
        val columnName = entry._1
        val columnMetrics = entry._2
        val columnQualifiedName = dbName + "." + tableName + "." + columnName + "@" + clusterName
        val hiveColumnReferenceable = new Referenceable(Constants.HiveColumnTypeName)
        val columnDataType = columnMetrics("type")

        hiveColumnReferenceable.set(Constants.TypeAttribute, columnDataType)
        hiveColumnReferenceable.set(Constants.QualifiedName, columnQualifiedName)
        hiveColumnReferenceable.set(Constants.Name, columnName)

        val columnProfileData = new Struct(Constants.HiveColumnProfileData)
        columnProfileData.set("nonNullCount", columnMetrics.get("count"))
        columnProfileData.set("cardinality", columnMetrics.get("distinct"))

        columnDataType match {
          case "int" | "long" | "double" | "decimal" | "float" => {
            columnProfileData.set("minValue", columnMetrics.get("min"))
            columnProfileData.set("maxValue", columnMetrics.get("max"))
            columnProfileData.set("meanValue", columnMetrics.get("mean"))
            columnProfileData.set("stdDeviation", columnMetrics.get("stddev"))
            columnProfileData.set("histogram", columnMetrics.get("histogram"))
            columnProfileData.set("frequentItems", columnMetrics.get("fq"))
            columnProfileData.set("quartiles", columnMetrics.get("quartiles"))
          }
          case "string" => {
            columnProfileData.set("lengthMinValue", columnMetrics.get("min"))
            columnProfileData.set("lengthMaxValue", columnMetrics.get("max"))
            columnProfileData.set("lengthMeanValue", columnMetrics.get("mean"))
            columnProfileData.set("lengthStdDeviation", columnMetrics.get("stddev"))
            columnProfileData.set("histogram", columnMetrics.get("fqh"))
            columnProfileData.set("frequentItems", columnMetrics.get("fq"))
          }

          case "boolean" => {
            columnProfileData.set("numTrue", columnMetrics.get("trueCount"))
            columnProfileData.set("numFalse", columnMetrics.get("falseCount"))
          }
          case a@_ =>
            logger.warn(s"Column type $a not supported for atlas")
        }

        hiveColumnReferenceable.set(Constants.ProfileDataAttr, columnProfileData)
        val columnReferenceableJSON = InstanceSerialization._toJson(hiveColumnReferenceable)
        logger.debug("Column referenceable JSON => {}", columnReferenceableJSON)

        val columnGetParams = List((Constants.TypeAttribute, Constants.HiveColumnTypeName), ("property", "qualifiedName"), ("value", columnQualifiedName))
        val getColumnResponse = AtlasInterface.getData(entityURL, atlasRestRequestParams, columnGetParams)

        getColumnResponse.code match {
          case 200 => {
            logger.info("Column with qualifiedName {} already exists in Atlas. Updating the profile data", columnQualifiedName)
            val response = AtlasInterface.postDataPartialUpdate(partialUpdatePostURL, atlasRestRequestParams, columnReferenceableJSON, "hive_column",
              "qualifiedName", columnQualifiedName)
            response.code match {
              case 200 => {
                logger.info("Update profile data request successful for column {} . Response => {}", columnQualifiedName,
                  response.body)
              }
              case _ => {
                logger.warn("Update profile data request failed for column {}. Response => {}", columnQualifiedName,
                  response.body)
              }
            }
          }
          case 404 => {
            if(getTableResponse.code == 200){
              logger.warn("Column with qualifiedName {} not found but entry for corresponding table {} found. Skipping",
                columnQualifiedName, tableQualifiedName)
            }else {
              logger.info("Column with qualifiedName {}  doesn't exist. It will be registered along with the table",
                columnQualifiedName)
            }
          }
          case _ => {
            logger.warn("Unexpected response => {} ", getColumnResponse.body)
          }
        }
        hiveColumnReferenceable
      }
    }

    if (getTableResponse.code == 404) {
      import collection.JavaConverters._
      hiveTableReferenceable.set(Constants.ColumnsAtttribute, column_entities.toList.asJava)
      hiveTableReferenceable.set(Constants.DbAttribute, hiveDbEntity.getId)
      logger.debug("Constructed table entity => {}", hiveTableReferenceable.toString)

      logger.info("Attempting to serialize entities for table {}", tableQualifiedName)
      val requestJSON = InstanceSerialization._toJson(List(hiveDbEntity, hiveTableReferenceable).asJava)
      logger.info("Serialized JSON for persistence request => {}", requestJSON)

      val response = AtlasInterface.postData(entityURL, atlasRestRequestParams, requestJSON)
      response.code match {
        case 201 => {
          logger.info("Create request successful . Response => {}", response.body)
        }
        case _ => {
          logger.info("Create request failed with error {}", response.body)
        }
      }
    }
  }
}

object AtlasInterface {

  def getData(url:String, headers:List[(String, String)], params:List[(String, String)]):HttpResponse[String] = {
    Http(url).timeout(connTimeoutMs = 600000, readTimeoutMs = 600000).headers(headers).params(params).asString
  }

  def postData(url:String, params:List[(String, String)], requestJSON:String):HttpResponse[String] = {
    Http(url).timeout(connTimeoutMs = 600000, readTimeoutMs = 600000).headers(params).postData(requestJSON).asString
  }

  def postDataPartialUpdate(url:String, params:List[(String, String)], requestJSON:String, typeName:String, name:String, value:String):
  HttpResponse[String] ={
    Http(url).timeout(connTimeoutMs = 600000, readTimeoutMs = 600000).headers(params).param("type", typeName).param("property", name).param("value", value).postData(requestJSON).asString
  }

  def putData(url:String, params:List[(String, String)], requestJSON:String):HttpResponse[String] = {
    Http(url).timeout(connTimeoutMs = 600000, readTimeoutMs = 600000).headers(params).postData(requestJSON).method("PUT").asString
  }
}

