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
  * Created by visharma on 7/7/17.
  */
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.json4s.native.Json
import org.json4s.DefaultFormats
import com.hortonworks.dataplane.profilers.atlas.commons.{AtlasInterface, Constants, Params}
import com.typesafe.scalalogging.LazyLogging
import org.apache.atlas.typesystem.json.InstanceSerialization
import org.apache.atlas.typesystem.{Referenceable, Struct}

case class HistogramEntries(bin: Any, count: Long)
case class LatLong(latitude:Double, longitude:Double)

object AtlasHandler extends LazyLogging{
  def persistToAtlas(atlasURL:String, colQualifiedName:String, latLongAggData:String) = {
    val partialUpdatePostURL = atlasURL + Constants.atlasEntityPartialUpdateRESTEndpoint
    val hiveColumnReferenceable = new Referenceable(Constants.HiveColumnTypeName)
    hiveColumnReferenceable.set(Constants.QualifiedName, colQualifiedName)
    val columnProfileData = new Struct(Constants.HiveColumnProfileData)
    columnProfileData.set("locationAggregateData", latLongAggData)
    hiveColumnReferenceable.set(Constants.ProfileDataAttr, columnProfileData)
    val columnReferenceableJSON = InstanceSerialization._toJson(hiveColumnReferenceable)
    val response = AtlasInterface.postDataPartialUpdate(partialUpdatePostURL, Params.AtlasPostRequestParams,
      columnReferenceableJSON, Constants.HiveColumnTypeName, Constants.QualifiedName, colQualifiedName)
  }
}

object LatLongAggregator extends App{
  latlongAnalysis()

  def latlongAnalysis():Unit = {
    val hiveContext = SparkSession.builder().appName("LatLongAggregator").enableHiveSupport().getOrCreate()
    val argList = args
    val db = argList(0)
    val table = argList(1)
    val clusterName = argList(2)
    val latitudeColumn = argList(3)
    val longitudeColumn = argList(4)
    val atlasURL = argList(5)

    val dataFrame = hiveContext.sql("select * from " + db + "." + table)

    val dataFrameAsRDD = dataFrame.select(latitudeColumn, longitudeColumn).rdd.cache()
    val parsedData = dataFrameAsRDD.map(row => Vectors.dense(Array(row.getDouble(0), row.getDouble(1))))

    val numClusters = 10
    val numIterations = 10
    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    val WSSSE = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)

    clusters.clusterCenters.foreach(println)
    val latLongHistogram = clusters.clusterCenters.map(latlong => {
      val filteredRows = dataFrameAsRDD.filter(row =>
        haversineDistance(latlong(0), latlong(1), row.getDouble(0), row.getDouble(1)) < 100).count()
      HistogramEntries(LatLong(latlong(0), latlong(1)), filteredRows)
    })
    val aggDataAsJSON = Json(DefaultFormats).write(latLongHistogram)
    val colQualifiedName = db + "." + table + "." + latitudeColumn + "@" + clusterName
    AtlasHandler.persistToAtlas(atlasURL, colQualifiedName, aggDataAsJSON)
    hiveContext.stop()
  }

  def eucliedeanDistance(x:Double, y:Double, a:Double, b:Double):Double = {
    val result = scala.math.pow(scala.math.pow(x - a, 2) + scala.math.pow(y-b, 2), 0.5)
    result
  }

  def haversineDistance(x:Double, y:Double, a:Double, b:Double):Double = {
    //Distance(/in kms) based on Haversine Formula https://en.wikipedia.org/wiki/Haversine_formula
    val p = scala.math.Pi / 180  // Pi/180 to convert angles in degree to radian
    val d = 0.5 - scala.math.cos((a - x) * p)/2 + scala.math.cos(x * p) * scala.math.cos(a * p) *
      (1 - scala.math.cos((b - y) * p)) / 2
    12742 * scala.math.asin(scala.math.sqrt(d))
  }
}
