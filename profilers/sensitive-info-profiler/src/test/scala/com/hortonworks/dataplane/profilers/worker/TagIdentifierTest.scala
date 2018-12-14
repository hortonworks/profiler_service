package com.hortonworks.dataplane.profilers.worker

import com.hortonworks.dataplane.profilers.atlas.interactors.HiveTable
import com.hortonworks.dataplane.profilers.kraptr.dsl.TagCreator
import com.hortonworks.dataplane.profilers.worker.DefaultInventories._
import org.apache.spark.sql.functions
import org.apache.spark.sql.types.{DataType, IntegerType, StringType}
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterAll, Matchers}


class TagIdentifierTest extends AsyncFlatSpec with Matchers with BeforeAndAfterAll {

  val systemThreshold = 70


  case class ColumnNames(idName: String = "id",
                         ipAddressName: String = "ipaddress",
                         ageName: String = "age",
                         emailName: String = "email",
                         ibanName: String = "iban") {
    def getNames: List[(String, String)] =
      List(
        ("id", idName),
        ("ipaddress", ipAddressName),
        ("age", ageName),
        ("email", emailName),
        ("iban", ibanName)
      )
  }


  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql(s"drop database IF EXISTS sensitivity_profiler_test cascade")
    spark.sql("create database sensitivity_profiler_test")
  }

  "SensitiveTableProfiler" should "be able to identify tag based on column type" in {

    val profilerResults: List[TableResult] = buildTableResultsFromDefaultData("tbl1",
      100, 30, 0)
    assert(profilerResults.exists(x => x.column == "email" && x.label == "email"))
    assert(profilerResults.exists(x => x.column == "email" && x.label == "email2"))
    assert(profilerResults.exists(x => x.column == "ipaddress" && x.label == "ipaddress"))
    assert(profilerResults.exists(x => x.column == "iban" && x.label == "SWE_IBAN_Detection"))
    assert(profilerResults.length == 4)
  }


  "SensitiveTableProfiler" should "be able to identify tag even when sample size is more than table size" in {

    val profilerResults: List[TableResult] = buildTableResultsFromDefaultData("tbl2", 30,
      100, 0)
    assert(profilerResults.exists(x => x.column == "email" && x.label == "email"))
    assert(profilerResults.exists(x => x.column == "email" && x.label == "email2"))
    assert(profilerResults.exists(x => x.column == "ipaddress" && x.label == "ipaddress"))
    assert(profilerResults.exists(x => x.column == "iban" && x.label == "SWE_IBAN_Detection"))
    assert(profilerResults.length == 4)
  }

  "SensitiveTableProfiler" should "not  fail or identify tags  on empty table" in {

    val profilerResults: List[TableResult] = buildTableResultsFromDefaultData("tbl3", 0,
      30, 0)
    assert(profilerResults.isEmpty)
  }

  "SensitiveTableProfiler" should "not  fail or identify tags when sample size is zero" in {

    val profilerResults: List[TableResult] = buildTableResultsFromDefaultData("tbl13", 100,
      0, 0)
    assert(profilerResults.isEmpty)
  }


  "SensitiveTableProfiler" should "not identify tags  on table with less than requiredMatches" in {

    val profilerResults: List[TableResult] = buildTableResultsFromDefaultData("tbl4", 50,
      100, 500)
    assert(profilerResults.isEmpty)
  }

  "SensitiveTableProfiler" should "identify tags  on table with small number of non-matching data" in {

    val profilerResults: List[TableResult] = buildTableResultsFromDefaultData("tbl5", 500,
      100, 5)
    assert(profilerResults.exists(x => x.column == "email" && x.label == "email"))
    assert(profilerResults.exists(x => x.column == "email" && x.label == "email2"))
    assert(profilerResults.exists(x => x.column == "ipaddress" && x.label == "ipaddress"))
    assert(profilerResults.exists(x => x.column == "iban" && x.label == "SWE_IBAN_Detection"))
    assert(profilerResults.length == 4)
  }


  "SensitiveTableProfiler" should "not tag columns where threshold is not met (driven by regex)" in {

    val goodRows = ((systemThreshold - 50) * 100 / 50) - 1
    val profilerResults: List[TableResult] = buildTableResultsFromDefaultData("tbl6", goodRows,
      100, 100 - goodRows,
      dslWithModifiedConfidences(emailWegihts = (50, 50)))
    assert(profilerResults.isEmpty)
  }

  "SensitiveTableProfiler" should "tag columns where threshold is met (driven by regex)" in {

    val goodRows = ((systemThreshold - 50) * 100 / 50) + 1
    val profilerResults: List[TableResult] = buildTableResultsFromDefaultData("tbl7", goodRows,
      100, 100 - goodRows,
      dslWithModifiedConfidences(emailWegihts = (50, 50)))
    assert(profilerResults.exists(x => x.column == "email" && x.label == "email"))
    assert(profilerResults.length == 1)
  }

  "SensitiveTableProfiler" should "tag columns where threshold (driven by regex) " +
    "is met and column name has no contribution" in {

    val goodRows = systemThreshold + 1
    val profilerResults: List[TableResult] = buildTableResultsFromDefaultData("tbl13", goodRows,
      100, 100 - goodRows,
      dslWithModifiedConfidences(emailWegihts = (0, 100)))
    assert(profilerResults.exists(x => x.column == "email" && x.label == "email"))
    assert(profilerResults.exists(x => x.column == "email" && x.label == "email2"))
    assert(profilerResults.exists(x => x.column == "ipaddress" && x.label == "ipaddress"))
    assert(profilerResults.length == 3)
  }


  "SensitiveTableProfiler" should "tag columns when threshold (driven by column name) is met" in {
    val profilerResults: List[TableResult] = buildTableResultsFromDefaultData("tbl8", 0,
      100, 50,
      dslWithModifiedConfidences(emailWegihts = (systemThreshold + 1, 100 - systemThreshold - 1)))
    assert(profilerResults.exists(x => x.column == "email" && x.label == "email"))
    assert(profilerResults.length == 1)
  }

  "SensitiveTableProfiler" should "not tag columns when threshold (driven by column name) is not met" in {
    val profilerResults: List[TableResult] = buildTableResultsFromDefaultData("tbl9", 0,
      100, 50,
      dslWithModifiedConfidences(emailWegihts = (systemThreshold - 1, 100 - systemThreshold + 1)))
    assert(profilerResults.isEmpty)
  }


  "SensitiveTableProfiler" should "not tag when column name is only enough to tag but there is no match" in {
    val profilerResults: List[TableResult] = buildTableResultsFromDefaultData("tbl10", 0,
      100, 50,
      dslWithModifiedConfidences(emailWegihts = (100, 0)),
      ColumnNames(emailName = "randomString"))
    assert(profilerResults.isEmpty)
  }

  "SensitiveTableProfiler" should "tag columns without failing when data has null values of String type" in {
    val profilerResults: List[TableResult] = buildDataAndAssignNullToColumn("tbl11",
      "email", StringType)
    assert(profilerResults.exists(x => x.column == "ipaddress" && x.label == "ipaddress"))
    assert(profilerResults.exists(x => x.column == "iban" && x.label == "SWE_IBAN_Detection"))
    assert(profilerResults.length == 2)
  }

  "SensitiveTableProfiler" should "tag columns without failing when data has null values of Integer type" in {
    val profilerResults: List[TableResult] = buildDataAndAssignNullToColumn("tbl12",
      "age", IntegerType)
    assert(profilerResults.exists(x => x.column == "email" && x.label == "email"))
    assert(profilerResults.exists(x => x.column == "email" && x.label == "email2"))
    assert(profilerResults.exists(x => x.column == "ipaddress" && x.label == "ipaddress"))
    assert(profilerResults.exists(x => x.column == "iban" && x.label == "SWE_IBAN_Detection"))
    assert(profilerResults.length == 4)
  }

  private def buildDataAndAssignNullToColumn(tableName: String, columnName: String, dataType: DataType) = {
    val rows = RowsBuilder.buildRowsAndFillBadData(90, List("172.16.254.1", "127.0.0.1"), List(33, 34, 35),
      List("nancyjpowers@jourrapide.com", "thatguy@hortonworks.com"),
      List("SE4550000000058398257466")
      , TableRow(0, "hello", 100000, "hello", "hello"),
      10
    )
    import spark.implicits._

    val rawDf = rows.toDF()
    val finalDf = rawDf.withColumn(columnName, functions.lit(null).cast(dataType))
    finalDf.write.saveAsTable(s"sensitivity_profiler_test.$tableName")

    val profilerResults = TagIdentifier.profile(interface, HiveTable("sensitivity_profiler_test", tableName), 100,
      dslWithModifiedConfidences())

    spark.sql(s"drop table sensitivity_profiler_test.$tableName")
    profilerResults.get
  }

  private def buildTableResultsFromDefaultData(tableName: String, goodRows: Int, sampleSize: Int, badRows: Int,
                                               tagCreators: List[TagCreator] = dslWithModifiedConfidences(),
                                               columnNames: ColumnNames = ColumnNames(),
                                               badRow: TableRow = TableRow(0, "hello", 100000, "hello", "hello")
                                              ): List[TableResult] = {
    val rows = RowsBuilder.buildRowsAndFillBadData(goodRows, List("172.16.254.1", "127.0.0.1"), List(33, 34, 35),
      List("nancyjpowers@jourrapide.com", "thatgirl@hortonworks.com"),
      List("SE4550000000058398257466")
      , badRow,
      badRows
    )
    import spark.implicits._

    val rawDf = rows.toDF()
    val finalDf = columnNames.getNames.foldLeft(rawDf)(
      (renamedFrame, namePair) => {
        renamedFrame.withColumnRenamed(namePair._1, namePair._2)
      }
    )
    finalDf.write.saveAsTable(s"sensitivity_profiler_test.$tableName")

    val profilerResults = TagIdentifier.profile(interface, HiveTable("sensitivity_profiler_test", tableName), sampleSize, tagCreators)
    spark.sql(s"drop table sensitivity_profiler_test.$tableName")
    profilerResults.get
  }


  override def afterAll(): Unit = {
    super.afterAll()
    spark.sql("drop database sensitivity_profiler_test cascade")
  }
}
