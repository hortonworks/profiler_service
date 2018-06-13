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
  * Created by visharma on 4/21/17.
  */

import java.util
import java.util.NoSuchElementException

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.approx_count_distinct
import org.apache.hadoop.fs._

import scala.collection.mutable.Map
import org.json4s.native.Json
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods._
import com.typesafe.scalalogging.LazyLogging
import java.util.Calendar
import java.util.Base64
import java.nio.charset.StandardCharsets
/**
  * Below are the required metrics for the different data types
  * "IntegerType" | "DecimalType" -> ["min", "max", "mean", "stddev", "num_nulls", "distinct_count"]
  * "StringType"                  -> ["avg_len", "max_len", "min_len", "num_nulls", "distinct_count"]
  * "BooleanType"                 -> ["num_trues", "num_falses", "num_nulls"]
  *
  */


object ProfileTable extends App with LazyLogging{
  val argList = args
  val inputJSON = argList(0)
  implicit val formats = DefaultFormats
  val input = InputParser.get(inputJSON)

  logger.info("Got Atlas Url => {} ", input.atlasInfo.url)
  logger.info("Got Atlas Username => {}", input.atlasInfo.user)
  logger.info("Got Cluster Name => {}", input.clusterName)
  logger.info("Got Hive MetaStore Url => {}", input.metastoreUrl)
  logger.info("Got Sample Percent => {}", input.samplePercent.toString)

  val atlasAuthString = "Basic " + Base64.getEncoder.encodeToString((input.atlasInfo.user + ":" +
    input.atlasInfo.password).getBytes(StandardCharsets.UTF_8))
  val atlasRestRequestParams = ("Authorization", atlasAuthString)::Constants.AtlasPostRequestParams

  logger.debug("Attempting to create a Spark HiveContext")
  val hiveContext = input.kerberosCredentials.isSecured match {
    case "true" => {
      logger.info("Kerberos is enabled and hence fetching Hivemetastore keytab and principal")
      SparkSession.builder().appName("Profiler").config("hive.metastore.uris", input.metastoreUrl).
        config("hive.metastore.kerberos.keytab", input.kerberosCredentials.keytab).
        config("hive.metastore.kerberos.principal", input.kerberosCredentials.principal).
        config("hive.metastore.sasl.enabled" , true).
        enableHiveSupport().getOrCreate()
    }
    case _ => SparkSession.builder().appName("Profiler").config("hive.metastore.uris", input.metastoreUrl).
      enableHiveSupport().getOrCreate()
  }

  logger.debug("Spark HiveContext created successfully {}", hiveContext.toString)

  for (asset <- input.tables){
    val dbName = asset.database
    val tableName = asset.table
    logger.info("Collecting metrics for DB {} and Table {}", dbName, tableName)
    val hive_query = "select * from " + dbName + "." + tableName
    logger.info("Executing hive query {} ", hive_query)
    val sampleFraction = input.samplePercent.toDouble / 100.0
    val dataframe = hiveContext.sql(hive_query).sample(false, sampleFraction).cache()
    val samplingTime = Calendar.getInstance().getTimeInMillis.toString
    val tableRowCount = dataframe.count()

    //Get the schema map from the sample data
    val fields = dataframe.schema.fields

    var columnTypeMap = Map[String, String]()
    fields.foreach(field => columnTypeMap = columnTypeMap + (field.name.toString -> field.dataType.toString))

    var collectedMetrics = Map[String, Map[String, Any]]()

    val dataTypeColumnsMap = columnTypeMap.groupBy(e => DataType.getDataType(e._2)).map {
      case (k, vs) =>
        (k, vs.map(_._1))
    }

    val numericColList = dataTypeColumnsMap.get(DataType.Numeric).toList.flatten
    if (numericColList.length > 0) {
      collectedMetrics = collectedMetrics ++ NumericTypeHandler.computeBasicMetrics(dataframe,
        numericColList)
      collectedMetrics = NumericTypeHandler.computeDistributions(hiveContext, dataframe, numericColList,
        columnTypeMap, collectedMetrics)
    }
    //TODO : String columns have to be processed in a single batch as the numerical columns are done.
    dataTypeColumnsMap.get(DataType.String).toList.flatten foreach {
      columnName => {
        val basicMetricMap = StringTypeHandler.computeBasicMetrics(hiveContext, dataframe, columnName)
        val distributionMap = StringTypeHandler.computeDistributions(hiveContext, dataframe, columnName)
        collectedMetrics.put(columnName, basicMetricMap ++ distributionMap)
      }
    }

    //Boolean types have to be processed individually because we are grouping rows by their boolean values
    //to calculate counts
    dataTypeColumnsMap.get(DataType.Boolean).toList.flatten foreach {
      columnName => {
        collectedMetrics.put(columnName, BooleanTypeHandler.computeBasicMetrics(dataframe, columnName))
      }
    }

    val collectedMetricsWithType = collectedMetrics map {
      entry => {
        val columnName = entry._1
        logger.info("Processing metrics received from Spark Job for column {}", columnName)
        val values = entry._2
        val sparkColumnType = columnTypeMap(columnName)
        val metricsInCaseClassList = DataType.getDataType(sparkColumnType) match {
          case DataType.Numeric =>
            Utils.NumericMetrics(
              getMetricAsString(values("min")).getOrElse("0").toDouble,
              getMetricAsString(values("max")).getOrElse("0").toDouble,
              getMetricAsString(values("mean")).getOrElse("0").toString.toDouble,
              getMetricAsString(values("stddev")).getOrElse("0").toString.toDouble,
              getMetricAsString(values("histogram")).getOrElse(""),
              getMetricAsString(values("frequentItems")).getOrElse(""),
              getMetricAsString(values("quartiles")).getOrElse(""),
              sparkColumnType,
              getMetricAsString(values("count")).getOrElse("0").toLong,
              getMetricAsString(values("distinct_count")).getOrElse("0").toString.toLong)

          case DataType.String =>
            Utils.StringMetrics(
              getMetricAsString(values("min")).getOrElse("0").toLong,
              getMetricAsString(values("max")).getOrElse("0").toLong,
              getMetricAsString(values("mean")).getOrElse("0").toDouble,
              getMetricAsString(values("stddev")).getOrElse("0").toDouble,
              getMetricAsString(values("histogram")).getOrElse(""),
              getMetricAsString(values("frequentItems")).getOrElse(""),
              sparkColumnType,
              getMetricAsString(values("count")).getOrElse("0").toLong,
              getMetricAsString(values("distinct_count")).getOrElse("0").toLong)

          case DataType.Boolean => Utils.BooleanMetrics(
            getMetricAsString(values("num_trues")).getOrElse("0").toLong,
            getMetricAsString(values("num_falses")).getOrElse("0").toLong,
            sparkColumnType,
            getMetricAsString(values("count")).getOrElse("0").toLong,
            getMetricAsString(values("distinct_count")).getOrElse("0").toLong)
        }
        (columnName, metricsInCaseClassList)
      }
    }

    logger.info("Metric collection completed for DB {} and table {}", dbName, tableName)

    logger.info("Attempting to persist metrics to Atlas repository at {} on cluster {}", input.atlasInfo.url, input.clusterName)
    AtlasPersister.persistToAtlas(input.atlasInfo.url, atlasRestRequestParams, input.clusterName, dbName, tableName, collectedMetricsWithType, tableRowCount,
      samplingTime, input.samplePercent.toString)
  }

  def getMetricAsString(v:Any):Option[String] = {
    try{
      Some(v.toString)
    }catch {
      case e:Exception => None
    }
  }
}

object DataType extends Enumeration {
  type DataType = Value
  val Numeric, String, Boolean, Undefined = Value

  def getDataType(sparkType: String) = {
    sparkType match {
      case str if str.startsWith("IntegerType") | str.startsWith("DecimalType")
                  | str.startsWith("DoubleType") | str.startsWith("FloatType")
                  | str.startsWith("LongType") => DataType.Numeric
      case str if str.startsWith("StringType") => DataType.String
      case str if str.startsWith("BooleanType") => DataType.Boolean
      case _ => DataType.Undefined
    }
  }

//Map to get Hive DataType from corresponding Spark SQL type is hardcoded for now
//Needs to be revisited later to get the exact Hive type
  val sparkToHiveDataTypeMap = Map(("StringType" -> "string"), ("BooleanType" -> "boolean"),
    ("DoubleType" -> "double"), ("DecimalType" -> "decimal"), ("FloatType" -> "float"),
    ("LongType" -> "long"), ("IntegerType" -> "int"))
}

object NumericTypeHandler extends LazyLogging{
  def computeBasicMetrics(dataFrame: DataFrame, numericColumnList: List[String]): Map[String, Map[String, Any]] = {
    logger.info("Computing basic metrics for columns {}", numericColumnList.toString())
    var numericMetricMap = Map[String, Map[String, Any]]()
    numericColumnList foreach (columnName => numericMetricMap = numericMetricMap + (columnName -> Map[String, Any]()))
    val metrics = dataFrame.select(numericColumnList.head, numericColumnList.tail: _*).describe().collect()
    metrics.foreach(metric_row => {
      val metric_seq = metric_row.toSeq
      val metric_name = metric_seq.head
      val metric_values = metric_seq.takeRight(metric_seq.length - 1)
      var columnMetricMap = Map[String, String]()
      for (i <- 0 until metric_values.length) {
        numericMetricMap(numericColumnList(i)).put(metric_name.toString, metric_values(i))
      }
    })

    //distinct has to be calculated separately for each column
    numericColumnList foreach (
      columnName => {
        val distinctRowCount = dataFrame.select(approx_count_distinct(columnName, rsd=0.05)).collect()(0)(0)
        numericMetricMap(columnName).put("distinct_count", distinctRowCount)
      }
      )
    numericMetricMap
  }

  def computeDistributions(hiveSession:SparkSession, dataFrame: DataFrame, numericColumnList: List[String],
                           columnTypeMap:Map[String, String], numericMetricMap:Map[String, Map[String, Any]]):
                            Map[String, Map[String, Any]]  = {
    logger.info("Computing distributions for columns {}", numericColumnList.toString())
    import hiveSession.implicits._
    val jsonWriter = Json(DefaultFormats)
    numericColumnList foreach {
      columnName => {
        val columnType = columnTypeMap(columnName)
        try{
          val histogramOutput = dataFrame.select(columnName).map(value => {
            try {
              columnType match {
                case "IntegerType" => value.getInt(0)
                case "FloatType" => value.getFloat(0)
                case "DoubleType" => value.getDouble(0)
                case "LongType" => value.getLong(0)
                case _ => 0
              }
            } catch {
              case e: Exception => 0
            }
          }).rdd.histogram(10)
          val histogramArray = histogramOutput._1 zip histogramOutput._2
          val hisogramArrayCaseClass = histogramArray.map(entry => Utils.HistogramEntries(entry._1, entry._2))
          numericMetricMap(columnName).put("histogram", jsonWriter.write(hisogramArrayCaseClass))
        }catch {
          case e:Exception => numericMetricMap(columnName).put("histogram", "")
        }

          val frequentItems = dataFrame.stat.freqItems(List(columnName), 0.05).collect()
          numericMetricMap(columnName).put("frequentItems", frequentItems.mkString(",").stripPrefix("[WrappedArray").
            stripSuffix("]"))

          try {
            val quartilesArr = Array(0.0, 0.25, 0.50, 0.75, 0.99)
            val approxQuartiles = dataFrame.stat.approxQuantile(columnName, quartilesArr, 0.25)
            val zippedQuartiles = quartilesArr zip approxQuartiles
            val zippedQuartilesCaseClass = zippedQuartiles.map(entry => Utils.QuartileEntries(entry._1, entry._2))
            numericMetricMap(columnName).put("quartiles", jsonWriter.write(zippedQuartilesCaseClass))
          }catch {
            case e:Exception => numericMetricMap(columnName).put("quartiles", "")
          }
        }
      }
    numericMetricMap
  }
}

object StringTypeHandler extends LazyLogging{
  def computeBasicMetrics(hiveSession: SparkSession, dataFrame: DataFrame, colName: String): Map[String, Any] = {
    logger.info("Computing basic metrics for string column {}", colName)
    import hiveSession.implicits._
    val resultsMap = Map[String, Any]()
    val length_metrics = dataFrame.select(colName).map(
      row => row(0) match {
        case x: String => x.toString
        case null => new String()
      }).map(value => value.length).describe().collect()

    length_metrics.foreach(length_metric => resultsMap.put(length_metric(0).toString, length_metric(1)))
    val nonNullCount = dataFrame.select(colName).filter(dataFrame(colName).isNotNull).count()
    val distinctRowCount = dataFrame.select(approx_count_distinct(colName, rsd = 0.05)).collect()(0)(0)
    resultsMap.put("count", nonNullCount)
    resultsMap.put("distinct_count", distinctRowCount)
    resultsMap
  }

  def computeDistributions(hiveSession:SparkSession, dataFrame: DataFrame, colName:String):Map[String, Any] = {
    logger.info("Computing distributions for string column {}", colName)
    val distributionsMap = Map[String, Any]()
    import hiveSession.implicits._
    val jsonWriter = Json(DefaultFormats)
    try {
      val frequentItemsWithFrequency = dataFrame.select(colName).groupBy(colName).count().sort($"count".desc).take(20)
      val frequentItems = frequentItemsWithFrequency.map(row => row(0))
      val histogramArrayCaseClass = frequentItemsWithFrequency.map(row => Utils.HistogramEntries(row(0), row(1).toString.toLong))
      distributionsMap.put("frequentItems", frequentItems.mkString("(", ",", ")"))
      distributionsMap.put("histogram", jsonWriter.write(histogramArrayCaseClass))
    }catch {
      case e:Exception => {
        distributionsMap.put("frequentItems", "")
        distributionsMap.put("histogram", "")
      }
    }
    distributionsMap
  }
}

object BooleanTypeHandler extends LazyLogging{
  def computeBasicMetrics(dataFrame: DataFrame, colName: String): Map[String, Any] = {
    logger.info("Computing metrics for boolean column {}", colName)
    val resultMap = Map[String, Any]()
    resultMap.put("num_trues", 0)
    resultMap.put("num_falses", 0)
    resultMap.put("num_nulls", 0)
    var nonNullCount:Long = 0
    val grouped_by_boolean = dataFrame.select(colName).groupBy(colName).count().collect()
    grouped_by_boolean foreach {
      row =>
        row(0) match {
          case true => {
            val numTrues = row.getLong(1)
            resultMap.put("num_trues", numTrues)
            nonNullCount += numTrues
          }
          case false => {
            val numFalses = row.getLong(1)
            resultMap.put("num_falses", numFalses)
            nonNullCount += numFalses
          }
          case _ => resultMap.put("num_nulls", row.getLong(1))
        }
    }
    resultMap.put("count", nonNullCount)
    resultMap.put("distinct_count", grouped_by_boolean.length)
    resultMap
  }
}