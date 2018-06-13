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

/**
  * Created by visharma on 6/20/17.
  */

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.native.Json
import org.apache.atlas.typesystem.{Referenceable,Struct}
import org.apache.atlas.typesystem.json.InstanceSerialization
import com.hortonworks.dataplane.profilers.atlas.commons.{AtlasInterface,Constants, Params}

import opennlp.tools.namefind.NameFinderME
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import scala.collection.mutable.Map
import scala.collection.mutable.Set


import java.io.FileInputStream;

object LocationData {
  val locationKeywords = List("city", "zipcode", "town", "latitude", "longitude", "country", "address", "pincode",
    "street", "state", "nation")
}

object AtlasPersisterGeo{
  def persistLocationInfoToAtlas(atlasURL:String, locationColumns:Map[String, String], dbName:String,
                                 tableName:String, clusterName:String):Unit = {
    val partialUpdatePostURL = atlasURL + Constants.atlasEntityPartialUpdateRESTEndpoint
    locationColumns.foreach{
      case (k,v) => {
        val hiveColumnReferenceable = new Referenceable(Constants.HiveColumnTypeName)
        val columnQualifiedName = dbName + "." + tableName + "." + k + "@" + clusterName
        hiveColumnReferenceable.set(Constants.QualifiedName, columnQualifiedName)
        val columnProfileData = new Struct(Constants.HiveColumnProfileData)
        columnProfileData.set("locationKeyword", v)
        hiveColumnReferenceable.set(Constants.ProfileDataAttr, columnProfileData)

        val columnReferenceableJSON = InstanceSerialization._toJson(hiveColumnReferenceable)
        val response = AtlasInterface.postDataPartialUpdate(partialUpdatePostURL, Params.AtlasPostRequestParams,
          columnReferenceableJSON, Constants.HiveColumnTypeName, Constants.QualifiedName, columnQualifiedName)
      }
    }
  }
}

object GeoLocationProfiler extends App{
  val hiveContext = SparkSession.builder().appName("GeoLocationProfiler").enableHiveSupport().getOrCreate()
  val argList = args
  val db = argList(0)
  val table = argList(1)
  val clusterName = argList(2)
  val atlasURL = argList(3)

  val keywordLocationColumns = keyWordMatchLocationIdentifier(hiveContext)
  val foundColumns = keywordLocationColumns.keySet
  val semanticLocationColumns = semanticLocationIdentifier(hiveContext, foundColumns)
  val combinedLocationColumns = keywordLocationColumns ++ semanticLocationColumns
  println("Location results => " + Json(DefaultFormats).write(combinedLocationColumns))
  AtlasPersisterGeo.persistLocationInfoToAtlas(atlasURL, combinedLocationColumns, db, table, clusterName)


  def keyWordMatchLocationIdentifier(hiveContext:SparkSession):Map[String, String] = {
    val hiveQuery = "describe " + db + "." + table
    val dataframe = hiveContext.sql(hiveQuery)
    val columnMetadata = dataframe.collect()
    var locationColumnMap = Map[String, String]()
    val locationColumns = columnMetadata.foreach(col => {
      val columnName = col(0).toString
      val columnDescription = col(2)
      val descriptionTokens = columnDescription match {
        case x:String => x.split(" ").toList
        case _ => List("")
      }
      LocationData.locationKeywords.foreach(locationKeyword => {
        val lcs = StringComparisonAlgos.longestCommonSubstring(locationKeyword, columnName)
        println("LCS of " + locationKeyword +  " and " + columnName + " is => " + lcs)
        val substringLength = lcs.length
        val minStrLength = scala.math.min(locationKeyword.length, columnName.length)
        if (substringLength == minStrLength){
          locationColumnMap.put(columnName.toLowerCase, locationKeyword)
        }else{
          val descriptionMatchResult = matchDescriptionTokens(locationKeyword, descriptionTokens)
          descriptionMatchResult match {
            case keyWord:Some[String] => locationColumnMap.put(columnName.toLowerCase, keyWord.get)
            case _ => println("No match found in description for location keyword " + locationKeyword)
          }
        }
      })
    })
    locationColumnMap
  }


  def matchDescriptionTokens(locationToken:String, descriptionTokens:List[String]):Option[String] = {
    var resultKeyword = None:Option[String]
    descriptionTokens.foreach(token => {
        val substringLength = StringComparisonAlgos.longestCommonSubstring(token, locationToken).length
        val minStringLength = scala.math.min(token.length, locationToken.length)
        if (substringLength > 0 && substringLength == minStringLength){
          resultKeyword = Some(locationToken)
        }
    })
    return resultKeyword
  }

  def semanticLocationIdentifier(hiveSession:SparkSession, keywordColumns:scala.collection.Set[String]):Map[String, String] = {
    var semanticLocationColumnMap = Map[String, String]()
    val foundColumns = Set[String]()  ++ keywordColumns
    val url = getClass.getResource("/en-token.bin")
    val inputStreamTokenizer = new FileInputStream(url.getPath)
    val tokenizerModel = new TokenizerModel(inputStreamTokenizer)
    val tokenizerME = new TokenizerME(tokenizerModel)

    val new_url = getClass.getResource("/en-ner-location.bin")
    val inputStreamNameFinder = new FileInputStream(new_url.getPath)
    val tokenNameFinderModel = new TokenNameFinderModel(inputStreamNameFinder)
    val nameFinderME = new NameFinderME(tokenNameFinderModel)

    val latitudeRegex = """^[-+]?([1-8]?\d(\.\d+)?|90(\.0+)?)""".r
    val longitudeRegex = """\s*[-+]?(180(\.0+)?|((1[0-7]\d)|([1-9]?\d))(\.\d+)?)$""".r

    val dataQuery = "select * from " + db + "." + table + " limit 100"
    val dataFrame = hiveSession.sql(dataQuery)
    val fields = dataFrame.schema.fields
    val columnTypeMap = fields.map(field => {
      val columnName = field.name.toString
      if(!foundColumns.contains(columnName)) {
        (field.name.toString, field.dataType.toString)
      }else{
        ("", "")
      }
    })
    val columnTypeMapFiltered = columnTypeMap.filter(_ != ("", ""))
    columnTypeMapFiltered.foreach{column => {
      println("Processing column => ", column)
      val columnData = dataFrame.select(column._1).collect()
      val columnValueList = columnData.map(row => row.mkString(" "))
      val tokens = tokenizerME.tokenize(columnValueList.mkString(" "))
      val spans = nameFinderME.find(tokens)
      if (spans.length > 30) {
        semanticLocationColumnMap.put(column._1, "locationSemantic")
        foundColumns.add(column._1)
      }
      if (!foundColumns.contains(column._1)) {
        column._2 match {
          case columnType if columnType.startsWith("DoubleType") | columnType.startsWith("FloatType") => {
            var latitudeCount = 0
            var longitudeCount = 0
            dataFrame.select(column._1).collect().map(row => {
              val columnVal = columnType match {
                case str if str.startsWith("DoubleType") => row.getDouble(0)
                case str if str.startsWith("FloatType") => row.getFloat(0)
              }

              columnVal.toString match {
                case longitudeRegex(_*) => {
                  longitudeCount += 1
                  columnVal.toString match {
                    case latitudeRegex(_*) => latitudeCount += 1
                    case _ =>
                  }
                }
                case _ =>
              }
            })
            println("Latitude matches " + latitudeCount + " , Longitude matches " + longitudeCount)
            if (latitudeCount == 100) {
              semanticLocationColumnMap.put(column._1, "latitude")
            }
            else if (longitudeCount == 100) {
              semanticLocationColumnMap.put(column._1, "longitude")
            }
          }
          case _ => println("Nothing to do")
        }
      }
    }
    }
  semanticLocationColumnMap
  }
}