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
import org.apache.spark.sql.SparkSession
import org.json4s.native.Json
import org.json4s.DefaultFormats

case class LocationPair(firstEntry:Any, secondEntry:Any)
case class PairDetails(firstColumn:String, secondColumn:String)

object LocationAggregator extends App{
  locationAggregation()

  def locationAggregation():Unit = {
    val hiveContext = SparkSession.builder().appName("LocationAggregator").enableHiveSupport().getOrCreate()
    val argList = args
    val db = argList(0)
    val table = argList(1)
    val clusterName = argList(2)
    val granularlocationColumn = argList(3)
    val locationColumn = argList(4)
    val atlasURL = argList(5)

    val dataFrame = hiveContext.sql("select * from " + db + "." + table)
    import hiveContext.implicits._
    val frequentItemsWithFrequency = dataFrame.select(granularlocationColumn, locationColumn)
      .groupBy(granularlocationColumn, locationColumn)
      .count().sort($"count".desc).take(10)

    val histogramArrayCaseClass = frequentItemsWithFrequency.map(row =>
      HistogramEntries(LocationPair(row(0), row(1)), row(2).toString.toLong))
    val pairDetails = PairDetails(granularlocationColumn, locationColumn)
    val jsonWriter = Json(DefaultFormats)
    val aggDataAsJSON =  jsonWriter.write(pairDetails) + " " + jsonWriter.write(histogramArrayCaseClass)
    val colQualifiedName = db + "." + table + "." + granularlocationColumn + "@" + clusterName
    AtlasHandler.persistToAtlas(atlasURL, colQualifiedName, aggDataAsJSON)
    hiveContext.stop()
  }

}
