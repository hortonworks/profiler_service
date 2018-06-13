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

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.LongType
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSpec}

import scala.util.Random

class SingleDFQueryTimeTest extends FunSpec with BeforeAndAfter with BeforeAndAfterAll{

  def createRandomDF(session:SparkSession, num:Int):RDD[Row] = {
    println("Creating rdd number " + num)
    import session.sqlContext.implicits._
    Seq((Utils.getRandomDB(), Utils.getRandomTable(), Utils.getRandomString(), Utils.getRandomLabel(),
      Random.nextInt(100).toLong)).toDF("db", "table", "column", "label", "percentMatch").rdd
  }

  var session:SparkSession = SparkSession.builder().appName("SensitiveProfilerScaleTesting").getOrCreate()
  import org.apache.spark.sql.types.{StringType, StructField, StructType}
  val schema = Array(("db", StringType), ("table", StringType), ("column", StringType),
    ("label", StringType), ("percentMatch", LongType))
  val colNames = schema.map(field => field._1)
  val schemaRDD = StructType(schema.map(field => StructField(field._1, field._2, true)))
  var startTime = System.currentTimeMillis()
  var endTime = System.currentTimeMillis()

  override def beforeAll():Unit = {
    val rddList = for (i <- 1 to 100000) yield createRandomDF(session, i)
    val mergedDF = session.sparkContext.union(rddList)
    val compressedDF = session.sqlContext.createDataFrame(mergedDF, schemaRDD).coalesce(5).cache()
    compressedDF.write.mode(SaveMode.Overwrite).orc("/tmp/sensitiveProfilerTest/SingleDFTest")
    val readDF = session.sqlContext.read.orc("/tmp/sensitiveProfilerTest/SingleDFTest").cache()
    readDF.createOrReplaceTempView("sensitiveInfo")
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

  describe("Query on dataframe ") {
    it(" for top 10 sensitive databases should be less than 20s ") {
      session.sqlContext.sql("select database, count(*) as count from sensitiveInfo group by database order by count desc limit 10").
        collect().map(println)
      assert((endTime - startTime) / 1000.0 < 5)

      it(" for top 10 sensitive database, label pairs should be less than 20s  ") {
        session.sqlContext.sql("select database, label, count(*) as count from sensitiveInfo group by database, label order by " +
          "count desc limit 10").collect().map(println)
        assert((endTime - startTime) / 1000.0 < 5)
      }

      it(" for top 10 sensitive tables should be less than 20s  ") {
        session.sqlContext.sql("select table, count(*) as count from sensitiveInfo group by table order by " +
          "count desc limit 10").collect().map(println)
        assert((endTime - startTime) / 1000.0 < 5)
      }

      it(" for top 10 sensitive table, label pair should be less than 20s  ") {
        session.sqlContext.sql("select table, label, count(*) as count from sensitiveInfo group by table, label " +
          "order by count desc limit 10").collect().map(println)
        assert((endTime - startTime) / 1000.0 < 5)
      }
    }
  }
}