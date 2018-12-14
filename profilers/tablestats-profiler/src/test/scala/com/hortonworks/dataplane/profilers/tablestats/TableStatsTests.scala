package com.hortonworks.dataplane.profilers.tablestats

import com.hortonworks.dataplane.profilers.hdpinterface.spark.SimpleSpark
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.json4s.jackson.JsonMethods._
import org.json4s.{DefaultFormats, JValue}
import org.scalatest.{Assertion, AsyncFlatSpec, BeforeAndAfterAll, Matchers}

import scala.collection.immutable


class TableStatsTests extends AsyncFlatSpec with Matchers with BeforeAndAfterAll {

  implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

  val sample_count_threshold: Float = 0.2f

  val statistical_result_threshold = 0.05

  val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()

  val interface: SimpleSpark = new SimpleSpark(spark)


  import spark.implicits._

  val testDatabase: String = "tablestats_profiler_test"


  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql(s"drop database IF EXISTS $testDatabase cascade")
    spark.sql(s"create database $testDatabase")
  }


  "TableStatsProfiler" should "not fail on empty table" in {
    val rows = buildRowsFromDefaultData(0)
    assert(runProfilerAndCollectMetrics(rows.toDF()).nonEmpty)
  }


  "TableStatsProfiler" should "take sample with size with threshold 20%" in {


    val rows = buildRowsFromDefaultData(1000)
    rows.toDF().write.saveAsTable(s"$testDatabase.tbl1")

    val df = TableStats.getTableDF(interface, testDatabase, "tbl1", 50)

    validateAccuracy(df.count().toFloat, 500.0, sample_count_threshold)

  }

  "TableStatsProfiler" should "calculate count,mean,stddev,min and max correctly for Numeric columns" in {
    val rows = buildRowsFromDefaultData(10000)
    val metrics: Map[String, Map[String, JValue]] = runProfilerAndCollectMetrics(rows.toDF())

    val meanAge = StatisticalFunctions.mean(rows.map(_.age))
    val stdAge = StatisticalFunctions.stdDev(rows.map(_.age))
    val maxAge = rows.map(_.age).max
    val minAge = rows.map(_.age).min

    val meanId = StatisticalFunctions.mean(rows.map(_.id))
    val stdId = StatisticalFunctions.stdDev(rows.map(_.id))
    val maxId = rows.map(_.id).max
    val minId = rows.map(_.id).min


    assert(rows.size == metrics("count")("count").extract[Int])
    validateAccuracy(metrics("age")("mean").extract[Double], meanAge)
    validateAccuracy(metrics("age")("stddev").extract[Double], stdAge)
    validateAccuracy(metrics("age")("max").extract[Double], maxAge)
    validateAccuracy(metrics("age")("min").extract[Double], minAge)

    validateAccuracy(metrics("id")("mean").extract[Double], meanId)
    validateAccuracy(metrics("id")("stddev").extract[Double], stdId)
    validateAccuracy(metrics("id")("max").extract[Double], maxId)
    validateAccuracy(metrics("id")("min").extract[Double], minId)
  }


  "TableStatsProfiler" should "determine distinct count of all columns correctly" in {
    val rows = buildRowsFromDefaultData(10000)
    val metrics: Map[String, Map[String, JValue]] = runProfilerAndCollectMetrics(rows.toDF())

    val distinctCountAge = rows.map(_.age).distinct.size
    val distinctCountId = rows.map(_.id).distinct.size
    val distinctCountEmail = rows.map(_.email).distinct.size
    val distinctCountIban = rows.map(_.iban).distinct.size

    validateAccuracy(metrics("age")("distinct").extract[Double], distinctCountAge)
    validateAccuracy(metrics("id")("distinct").extract[Double], distinctCountId)
    validateAccuracy(metrics("email")("distinct").extract[Double], distinctCountEmail)
    validateAccuracy(metrics("iban")("distinct").extract[Double], distinctCountIban)
  }

  "TableStatsProfiler" should "determine number of null values and other statistics without failure" in {
    val rows = buildRowsFromDefaultData(10000)
    val tempDataframe = rows.toDF()
    val rows2 = buildRowsFromDefaultData(500)
    val combinedRows: List[TableRow] = rows ::: rows2
    val deltaDf = rows2.toDF().withColumn("age", functions.lit(null).cast(IntegerType))
      .withColumn("email", functions.lit(null).cast(StringType))
    val dataframe = deltaDf.union(tempDataframe)
    val metrics: Map[String, Map[String, JValue]] = runProfilerAndCollectMetrics(dataframe)


    val meanAge = StatisticalFunctions.mean(rows.map(_.age))
    val stdAge = StatisticalFunctions.stdDev(rows.map(_.age))
    val maxAge = rows.map(_.age).max
    val minAge = rows.map(_.age).min


    assert(combinedRows.size == metrics("count")("count").extract[Int])
    validateAccuracy(metrics("age")("mean").extract[Double], meanAge)
    validateAccuracy(metrics("age")("stddev").extract[Double], stdAge)
    validateAccuracy(metrics("age")("max").extract[Double], maxAge)
    validateAccuracy(metrics("age")("min").extract[Double], minAge)
    assert(rows2.size == metrics("age")("null").extract[Int])
    assert(rows2.size == metrics("email")("null").extract[Int])
  }

  "TableStatsProfiler" should "determine count of null values and other statistics without failure when all entries of a column is null" in {
    val rows2 = buildRowsFromDefaultData(500)
    val dataframe = rows2.toDF().withColumn("age", functions.lit(null).cast(IntegerType))
      .withColumn("email", functions.lit(null).cast(StringType))
    val metrics: Map[String, Map[String, JValue]] = runProfilerAndCollectMetrics(dataframe)

    assert(rows2.size == metrics("count")("count").extract[Int])
    validateAccuracy(metrics("age")("mean").extract[Double], 0d)
    validateAccuracy(metrics("age")("stddev").extract[Double], 0d)
    validateAccuracy(metrics("age")("max").extract[Double], Double.MinValue)
    validateAccuracy(metrics("age")("min").extract[Double], Double.MaxValue)
    assert(rows2.size == metrics("age")("null").extract[Int])
    assert(rows2.size == metrics("email")("null").extract[Int])
  }

  "TableStatsProfiler" should "determine types correctly" in {
    val rows = buildRowsFromDefaultData(10000)
    val metrics: Map[String, Map[String, JValue]] = runProfilerAndCollectMetrics(rows.toDF())

    assert("int" == metrics("id")("type").extract[String])
    assert("string" == metrics("email")("type").extract[String])
    assert("string" == metrics("ipaddress")("type").extract[String])
    assert("int" == metrics("age")("type").extract[String])
    assert("DateType" == metrics("date")("type").extract[String])
    assert("boolean" == metrics("isworking")("type").extract[String])
  }

  "TableStatsProfiler" should "determine Frequent Items within threshold" in {
    val rows = buildRowsFromDefaultData(10000)
    val metrics: Map[String, Map[String, JValue]] = runProfilerAndCollectMetrics(rows.toDF())

    val agesAndCounts = rows.map(_.age).groupBy(identity).mapValues(_.size)
      .toList.sortBy(_._2).reverse.take(5).toMap

    val fqhForAge = parse(metrics("age")("fqh").extract[String]).extract[List[Map[String, JValue]]]
    val fdhAgeCompressed = fqhForAge.map(
      x =>
        x("bin").extract[String].toInt -> x("count"
        ).extract[Int]).toMap


    val str = metrics("age")("fq").extract[String]
    val frequentAges = str.substring(1, str.length - 1).split(",").map(_.toInt)

    agesAndCounts.keys.foreach(
      age => {
        assert(frequentAges.contains(age))
      }
    )


    agesAndCounts.map(
      agesAndCount => {
        val countFromMetrics = fdhAgeCompressed(agesAndCount._1)
        validateAccuracy(countFromMetrics, agesAndCount._2)
      }
    ).head
  }

  "TableStatsProfiler" should "determine Histograms within threshold" in {
    val rows = buildRowsFromDefaultData(10000)
    val metrics: Map[String, Map[String, JValue]] = runProfilerAndCollectMetrics(rows.toDF())

    val ageHistograms = parse(metrics("age")("histogram").extract[String]).extract[List[Map[String, Double]]]

    val ageHistogramsCompressed = ageHistograms.map(x => (x("bin"), x("count")))
    val binsArranged: immutable.IndexedSeq[(Double, Double, Double)] = (0 until ageHistogramsCompressed.size - 1).map(
      x => (ageHistogramsCompressed(x)._1, ageHistogramsCompressed(x + 1)._1, ageHistogramsCompressed(x)._2)
    ) :+ (ageHistogramsCompressed.last._1, rows.map(_.age).max.toDouble + 1, ageHistogramsCompressed.last._2)

    val randomBins = scala.util.Random.shuffle(binsArranged).take(2)

    randomBins.map(
      bin => {
        val numberOfItemsInBinActual = rows.map(_.age).count(x => x >= bin._1 && x < bin._2)
        validateAccuracy(bin._3, numberOfItemsInBinActual)
      }
    ).head
  }

  "TableStatsProfiler" should "determine quartiles within threshold" in {
    val rows = buildRowsFromDefaultData(10000)
    val metrics: Map[String, Map[String, JValue]] = runProfilerAndCollectMetrics(rows.toDF())

    val idQuartiles = parse(metrics("id")("quartiles").extract[String]).extract[List[Map[String, Double]]]

    val idQuartilesCompressed = idQuartiles.map(x => x("quartile") -> x("value")).toMap

    val quartilesThreshold: Double = 0.1

    validateAccuracy(idQuartilesCompressed(0.25), 2500, quartilesThreshold)

    validateAccuracy(idQuartilesCompressed(0.5), 5000, quartilesThreshold)

    validateAccuracy(idQuartilesCompressed(0.75), 7500, quartilesThreshold)
  }


  private def validateAccuracy(actual: Double, expected: Double, threshold: Double = statistical_result_threshold): Assertion = {
    val res = if (expected != 0) ((actual / expected) - 1.0).abs else 0
    assert(res < threshold)
  }

  private def buildRowsFromDefaultData(numberOfRows: Int): List[TableRow] = {
    val rows = RowsBuilder.buildRowsOfSize(numberOfRows, List("172.16.254.1",
      "127.0.0.1", "127.0.0.2",
      "127.0.0.3", "127.0.0.4",
      "127.0.0.5", "127.0.0.6"),
      20 to 30 toList,
      List("nancyjpowers@jourrapide.com", "thatgirl@hortonworks.com",
        "thatgirl1@hortonworks.com", "thatgirl2@hortonworks.com", "thatguy2@hortonworks.com",
        "thatguy1@hortonworks.com", "thatguy3@hortonworks.com",
        "thatguy5@hortonworks.com"),
      List("SE4550000000058398257466", "SE4550000000058398257466",
        "SE4550000000058398557466", "SE4450000000058398557466",
        "SE445000000005839855767466", "SE445000000005739855767466",
        "SE4450000000057398576767466", "SE4480000000057398576767466")
    )
    rows
  }

  private def runProfilerAndCollectMetrics(rows: DataFrame) = {
    TableStats.profileDFFull(
      rows
    ).seq.map(
      x => {
        val stringToValue = x.toJson.extract[Map[String, JValue]]
        val stringToStringToValue = stringToValue.head._1 match {
          case "count" =>
            Map("count" -> Map("count" -> stringToValue.head._2))
          case _ =>
            Map(stringToValue.head._1 -> stringToValue.head._2.extract[Map[String, JValue]])
        }
        stringToStringToValue.head
      }
    ).groupBy(_._1).mapValues(x => x.flatMap(_._2).toMap)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    spark.sql(s"drop database $testDatabase cascade")
    spark.close()
  }
}
