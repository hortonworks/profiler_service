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

package com.hortonworks.dataplane.profilers.hdpinterface.impl.spark

import com.hortonworks.dataplane.profilers.hdpinterface.spark.SparkInterface
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class SparkInterfaceImpl(appName: String, config: Seq[(String, String)]) extends SparkInterface {

  private val spark: SparkSession = {
    val builder = SparkSession.builder()
      .appName(appName)
    config.foldLeft(builder)((builderAggregate, pair) => {
      builderAggregate.config(pair._1, pair._2)
    }).enableHiveSupport().getOrCreate()
  }

  override def getFileSystem: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

  override def executeQuery(sql: String): Dataset[Row] = spark.sql(sql)

  override def execute(sql: String): Dataset[Row] = spark.sql(sql)

  override def table(tableName: String): Dataset[Row] = spark.table(tableName)

  override def showDatabases(): Dataset[Row] = spark.sql("show databases")

  override def showTables(database: String): Dataset[Row] = {
    spark.sql(s"use $database")
    spark.sql("show tables").select("tableName", "database")
  }

  override def describeTable(tableName: String): Dataset[Row] = spark.sql(s"describe formatted $tableName")

  override def session: SparkSession = spark

  override def close(): Unit = spark.close()
}
