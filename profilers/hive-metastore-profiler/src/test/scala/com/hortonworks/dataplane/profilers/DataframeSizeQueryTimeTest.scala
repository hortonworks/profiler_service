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
package com.hortonworks.dataplane.profilers

import org.apache.spark.sql.types.{DateType, LongType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.util.Random
import java.text.SimpleDateFormat
import java.util.Calendar

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.scalatest.{BeforeAndAfter, FunSpec, BeforeAndAfterAll}

class DataframeSizeQueryTimeTest extends FunSpec with BeforeAndAfter with BeforeAndAfterAll with LazyLogging{
  val dateFormat = new SimpleDateFormat("yyyy-mm-dd")

  def getRandomString():String = {
    Random.alphanumeric.take(10).mkString
  }

  def getRandomLong():Long = {
    Random.nextLong()
  }

  def getRandomDate():String = {
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -3650)
    val numDays = Random.nextInt(3650)
    val randomDate = cal.add(Calendar.DATE, numDays)
    return dateFormat.format(cal.getTime)
  }

  def getRandomDB():String = {
    val dbList = for (i <- 1 to 20) yield "db_" + i
    dbList(Random.nextInt(19))
  }

  def getRandomInputFormat():String = {
    val formatList = for (i <- 1 to 10) yield "InputFormat_" + i
    formatList(Random.nextInt(9))
  }

  def getRandomOutputFormat():String = {
    val formatList = for (i <- 1 to 10) yield "OutputFormat_" + i
    formatList(Random.nextInt(9))
  }

  def createRandomDF(session:SparkSession, num:Int):RDD[Row] = {
    logger.debug("Creating dataframe number {}", num.toString)
    import session.sqlContext.implicits._
    Seq((getRandomString(), getRandomDB(), getRandomDate(), getRandomLong(), getRandomLong(), getRandomLong(),
      getRandomInputFormat(), getRandomOutputFormat())).toDF("tableQualifiedName", "db", "createTime", "size", "numRows",
      "numPartitions", "inputFormat", "outputFormat").rdd
  }

  var session:SparkSession = SparkSession.builder().appName("HiveMetastoreProfiler").getOrCreate()
  var startTime:Long = System.currentTimeMillis()
  var endTime:Long = System.currentTimeMillis()
  var mergedDF:RDD[Row] = session.sparkContext.emptyRDD
  var newDF:DataFrame = session.sqlContext.emptyDataFrame
  var readDF:DataFrame = session.sqlContext.emptyDataFrame

  override def beforeAll():Unit = {
    session =  SparkSession.builder().appName("HiveMetastoreProfiler").getOrCreate()
    mergedDF = session.sparkContext.emptyRDD
    newDF = session.sqlContext.emptyDataFrame
    readDF = session.sqlContext.emptyDataFrame
  }

  override def afterAll(): Unit = {
    session.stop()
  }

  before {
    startTime = System.currentTimeMillis()
  }

  after {
    endTime = System.currentTimeMillis()
  }

  import org.apache.spark.sql.types.{StructType,StructField,StringType}
  val schema = Array(("table", StringType), ("db", StringType), ("createTime", StringType),
    ("size", LongType), ("numRows", LongType), ("numPartitions", LongType),
    ("inputFormat", StringType), ("outputFormat", StringType))
  val colNames = schema.map(field => field._1)
  val schemaRDD = StructType(schema.map(field => StructField(field._1, field._2, true)))

  val dfList = for (i <- 1 to 50000) yield createRandomDF(session, i)

  describe("Dataframe Operation Execution Time"){
    it("RDD merge should be less than 1 second"){
      mergedDF = session.sparkContext.union(dfList)
      assert((endTime - startTime) / 1000.0 < 1)
    }

    it("RDD to Dataframe conversion should be less than 1 second"){
      newDF = session.sqlContext.createDataFrame(mergedDF, schemaRDD).coalesce(5).cache()
      assert((endTime - startTime) / 1000.0 < 1)
    }

    it("Dataframe write to HDFS should be less than 2 minutes"){
      newDF.write.mode(SaveMode.Overwrite).parquet("/tmp/cannedCollectionRange")
      assert((endTime - startTime) / 1000.0 < 120)
    }

    it("Dataframe read from HDFS should be less than 1 second"){
      readDF = session.sqlContext.read.parquet("/tmp/cannedCollectionRange").cache()
      assert((endTime - startTime) / 1000.0 < 1)
    }
  }

  describe("Canned Queries Execution Time"){
    it("Top Size Table Query should execute within 5 s"){
      CannedCollectionQueries.getQueryResults(session, readDF, MetadataQuery.TopSize).map(println)
      assert((endTime - startTime) / 1000.0 < 5)
    }

    it("Top Rows Table Query should execute within 5 s"){
      CannedCollectionQueries.getQueryResults(session, readDF, MetadataQuery.TopRows).map(println)
      assert((endTime - startTime) / 1000.0 < 5)
    }

    it("Top Partitions Table Query should execute within 5 s"){
      CannedCollectionQueries.getQueryResults(session, readDF, MetadataQuery.TopPartitions).map(println)
      assert((endTime - startTime) / 1000.0 < 5)
    }

    it("Top DBs with maximum tables Query should execute within 5 s"){
      CannedCollectionQueries.getQueryResults(session, readDF, MetadataQuery.Db).map(println)
      assert((endTime - startTime) / 1000.0 < 5)
    }

    it("Top InputFormats should execute within 5 s"){
      CannedCollectionQueries.getQueryResults(session, readDF, MetadataQuery.InputFormat).map(println)
      assert((endTime - startTime) / 1000.0 < 5)
    }

    it("Top OutputFormats Query should execute within 5 s"){
      CannedCollectionQueries.getQueryResults(session, readDF, MetadataQuery.OutputFormat).map(println)
      assert((endTime - startTime) / 1000.0 < 5)
    }

    it("Attribute ranges in DB Query should execute within 5 s"){
      CannedCollectionQueries.getQueryResults(session, readDF, MetadataQuery.Range).map(println)
      assert((endTime - startTime) / 1000.0 < 5)
    }
  }
}
