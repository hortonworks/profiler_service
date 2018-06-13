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
import com.hortonworks.dataplane.profilers.label.TableResultMini
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSpec}

class HybridDataframeQueryTimeTest extends FunSpec with BeforeAndAfter with BeforeAndAfterAll{
  val session:SparkSession = SparkSession.builder().appName("SensitiveProfilerScaleTesting").enableHiveSupport().getOrCreate()
  val baseHDFSPath = "/tmp/sensitiveProfilerTest/PartitionedDFTestPath"
  var startTime = System.currentTimeMillis()
  var endTime = System.currentTimeMillis()
  var dbName = ""
  var tableName = ""
  var savePath = ""


  override def beforeAll():Unit = {
    import session.implicits._
    for (i <- 1 to 50) {
      dbName = Utils.getRandomDB()
      for (j <- 1 to 200) {
        tableName = Utils.getRandomTable()
        savePath = s"$baseHDFSPath/database=$dbName/table=$tableName"
        val rowDF = for (k <- 1 to 100) yield TableResultMini(Utils.getRandomString(), Utils.getRandomLabel(),
          Utils.getRandomString())
        rowDF.toDF("column", "label", "percentMatch").write.mode(SaveMode.Overwrite).parquet(savePath)
      }
    }
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

  session.sqlContext.read.parquet("/tmp/sensitiveProfilerTest/PartitionedDFTestPath").
    toDF("column", "label", "percentMatch", "database", "table").coalesce(10).write.mode(SaveMode.Overwrite).parquet("/tmp/sensitiveProfilerTest/PartitionedDFMerged")
  val readDF = session.sqlContext.read.parquet("/tmp/sensitiveProfilerTest/PartitionedDFMerged").cache()
  readDF.createOrReplaceTempView("sensitiveInfoPartitioned")

  describe("Query on the dataframe ") {
    it(" for top 10 sensitive databases should be less than 20s ") {
      session.sqlContext.sql("select database, count(*) as count from sensitiveInfoPartitioned group by database order by count desc limit 10").
        collect().map(println)
      assert((endTime - startTime) / 1000.0 < 20)

      it(" for top 10 sensitive database, label pairs should be less than 20s  ") {
        session.sqlContext.sql("select database, label, count(*) as count from sensitiveInfoPartitioned group by database, label order by " +
          "count desc limit 10").collect().map(println)
        assert((endTime - startTime) / 1000.0 < 20)
      }

      it(" for top 10 sensitive tables should be less than 20s  ") {
        session.sqlContext.sql("select table, count(*) as count from sensitiveInfoPartitioned group by table order by " +
          "count desc limit 10").collect().map(println)
        assert((endTime - startTime) / 1000.0 < 20)
      }

      it(" for top 10 sensitive table, label pair should be less than 20s  ") {
        session.sqlContext.sql("select table, label, count(*) as count from sensitiveInfoPartitioned group by table, label " +
          "order by count desc limit 10").collect().map(println)
        assert((endTime - startTime) / 1000.0 < 20)
      }
    }
  }
}
