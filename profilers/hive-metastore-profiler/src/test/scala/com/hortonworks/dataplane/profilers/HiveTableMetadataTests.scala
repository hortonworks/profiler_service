package com.hortonworks.dataplane.profilers

import com.hortonworks.dataplane.profilers.hdpinterface.spark.SimpleSpark
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.scalatest._

class HiveTableMetadataTests extends AsyncFlatSpec with Matchers with BeforeAndAfterEach {


  val databases: List[String] = List("db1", "db2", "db3")

  val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()

  val interface: SimpleSpark = new SimpleSpark(spark)


  override def beforeEach(): Unit = {
    databases.foreach(database => {
      spark.sql(s"drop database IF EXISTS $database cascade")
      spark.sql(s"create database $database")
    })
    super.beforeEach()
  }


  "HiveTableMetadataProfiler" should "be able to identify all tables,partitions" in {
    val tables: Seq[RandomTable] = RandomDataWarehouseBuilder.generateTables(spark, 5, Some(databases))

    val metaDataDf: Dataset[Row] = HiveTableMetadata.collectMetadata(interface)

    val tablesAndPartitionsIdentifiedByMetastoreProfiler = metaDataDf.select("table", "numPartitions").collect().map(x => (x.getString(0), x.getLong(1))).toSet
    val tablesAndPartitionsExpected = tables.map(tbl => s"${tbl.database}.${tbl.table}" -> tbl.partitions.toLong).toSet
    assert(tablesAndPartitionsExpected == tablesAndPartitionsIdentifiedByMetastoreProfiler)
  }


  "HiveTableMetadataProfiler" should "not fail if there are no tables" in {
    val metaDataDf: Dataset[Row] = HiveTableMetadata.collectMetadata(interface)
    assert(metaDataDf.rdd.isEmpty())
  }


  override def afterEach(): Unit = {
    databases.foreach(database => spark.sql(s"drop database IF EXISTS $database cascade"))
    super.afterEach()
  }

}
