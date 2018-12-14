package com.hortonworks.dataplane.profilers.auditprofiler

import java.io.File

import com.hortonworks.dataplane.profilers.auditprofiler.aggregate.HiveRangerAggregator
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterAll, Matchers}

class QueryExecutorTest extends AsyncFlatSpec with Matchers with BeforeAndAfterAll {


  case class ActualDataAndAuditAggregate(actualData: Seq[AuditRow], auditAggregate: DataFrame)

  case class HiveTable(database: String, table: String)


  implicit val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()

  import spark.implicits._


  "HiveRangerAggregator" should "be able to read valid ranger audit log" in {
    val file: File = new File(getClass.getClassLoader.getResource("hiveServer2_ranger_audit_sample.log").getFile)
    val auditLogLocation: String = s"file://${file.getAbsolutePath}"
    val auditOutput = HiveRangerAggregator.aggregateDF(HiveDFTransformer.transform(spark.read.json(auditLogLocation)))
    assert(!auditOutput.rdd.isEmpty(), "AuditProfiler failed to profiler a valid audit log")
  }

  "HiveRangerAggregator" should "identify all events" in {
    val numberOfEvents = 400
    val actualDataAndAuditAggregate = simulateAndRunAuditAggregation(numberOfEvents)
    val calculatedEvents = actualDataAndAuditAggregate.auditAggregate.select(sum("count").as("count")).collect().head.getLong(0)
    assert(numberOfEvents == calculatedEvents, "AuditProfiler is not able to identify all valid events")
  }


  "HiveRangerAggregator" should "identify table level event count correctly" in {
    val numberOfEvents = 400
    val actualDataAndAuditAggregate = simulateAndRunAuditAggregation(numberOfEvents)
    val randomTables = pickRandomTables(actualDataAndAuditAggregate.actualData, 4)
    randomTables.map(
      table => {
        val numberOfEvents: Long = eventsOfThisTable(table, actualDataAndAuditAggregate.actualData).size
        val calculatedEvents: Long = actualDataAndAuditAggregate.auditAggregate.filter(col("database").equalTo(lit(table.database))
          .and(col("table").equalTo(lit(table.table)))
        ).
          select(sum("count").as("count")).collect().head.getLong(0)
        assert(calculatedEvents == numberOfEvents, s"number of events identified on a table is not the same as actual")
      }
    ).head
  }


  "HiveRangerAggregator" should "identify grouped access counts correctly at table level" in {
    val numberOfEvents = 400
    val actualDataAndAuditAggregate = simulateAndRunAuditAggregation(numberOfEvents)
    val randomTables = pickRandomTables(actualDataAndAuditAggregate.actualData, 4)
    randomTables.map(
      table => {
        val actualEventsForThisTable: Seq[AuditRow] = eventsOfThisTable(table, actualDataAndAuditAggregate.actualData)
        val accessAndCounts: Map[String, Int] = actualEventsForThisTable.map(_.access).groupBy(identity).mapValues(_.size)
        actualDataAndAuditAggregate.auditAggregate.filter(s"database = '${table.database}' and table = '${table.table}'")
          .select("data.access").as("access").registerTempTable("accesses")
        val accesseToCountCalculated = spark.sql("select explode(access) AS (access, count) from accesses").collect().map(
          x => x.getString(0) -> x.getInt(1)
        ).toMap
        assert(accesseToCountCalculated == accessAndCounts)
      }
    ).head
  }


  "HiveRangerAggregator" should "identify grouped action counts correctly at table level" in {
    val numberOfEvents = 400
    val actualDataAndAuditAggregate = simulateAndRunAuditAggregation(numberOfEvents)
    val randomTables = pickRandomTables(actualDataAndAuditAggregate.actualData, 4)
    randomTables.map(
      table => {
        val actualEventsForThisTable: Seq[AuditRow] = eventsOfThisTable(table, actualDataAndAuditAggregate.actualData)
        val actionAndCounts: Map[String, Int] = actualEventsForThisTable.map(_.action).groupBy(identity).mapValues(_.size)
        actualDataAndAuditAggregate.auditAggregate.filter(s"database = '${table.database}' and table = '${table.table}'")
          .select("data.action").as("action").registerTempTable("actions")
        val actionAndCountsCalculated = spark.sql("select explode(action) AS (action, count) from actions").collect().map(
          x => x.getString(0) -> x.getInt(1)
        ).toMap
        assert(actionAndCountsCalculated == actionAndCounts)
      }
    ).head
  }


  "HiveRangerAggregator" should "attribute a users activity correctly" in {
    val numberOfEvents = 400
    val actualDataAndAuditAggregate = simulateAndRunAuditAggregation(numberOfEvents)
    val randomTables = pickRandomTables(actualDataAndAuditAggregate.actualData, 4)
    randomTables.map(
      table => {
        val actualEventsForThisTable: Seq[AuditRow] = eventsOfThisTable(table, actualDataAndAuditAggregate.actualData)
        val actionAndUsers = actualEventsForThisTable.map(x => (x.action, x.access, x.reqUser))
        val actionAccessUser: (String, String, String) = RandomHiveAuditDataGenerator.rnd(actionAndUsers)
        val eventsOfOneUser = actualEventsForThisTable.filter(_.reqUser == actionAccessUser._3)
        val numberOfAccessesOfOneType = eventsOfOneUser.count(_.access == actionAccessUser._2)
        val numberOfActionsOfOneType = eventsOfOneUser.count(_.action == actionAccessUser._1)
        actualDataAndAuditAggregate.auditAggregate.filter(s"database = '${table.database}' and table = '${table.table}'").registerTempTable("temptable1")
        spark.sql("select  explode(user) AS (user, aggregates) from temptable1").filter(s"user=='${actionAccessUser._3}'").registerTempTable("tempTable2")
        spark.sql("select explode(aggregates.action) as (action,action_count) from tempTable2").registerTempTable("action_table")
        spark.sql("select explode(aggregates.access) as (access,access_count) from tempTable2").registerTempTable("access_table")
        val calculatedActionCount = spark.sql(s"select action_count from action_table where action='${actionAccessUser._1}'").collect().head.getInt(0)
        val calculatedAccessCount = spark.sql(s"select access_count from access_table where access='${actionAccessUser._2}'").collect().head.getInt(0)
        assert(numberOfAccessesOfOneType == calculatedAccessCount)
        assert(numberOfActionsOfOneType == calculatedActionCount)
      }
    ).head
  }

  private def eventsOfThisTable(table: HiveTable, actualData: Seq[AuditRow]): Seq[AuditRow] = {
    actualData.filter(
      x =>
        x.resource.startsWith(s"${table.database}/${table.table}/")
    )
  }


  private def pickRandomTables(actualData: Seq[AuditRow], numberOfTables: Int): Seq[HiveTable] = {
    val tables = actualData.map(x => {
      val strings = x.resource.split("/")
      val db = strings(0)
      val table = strings(1)
      HiveTable(db, table)
    }).distinct
    if (tables.size <= numberOfTables)
      tables
    else {
      val indicesToPick = scala.util.Random.shuffle(tables.indices.toList).slice(0, numberOfTables - 1)
      indicesToPick.map(tables)
    }
  }

  private def simulateAndRunAuditAggregation(numberOfEvents: Int): ActualDataAndAuditAggregate = {
    val auditRows: Seq[AuditRow] = RandomHiveAuditDataGenerator.generateRandomAuditData(numberOfEvents)
    val auditAggregate = HiveRangerAggregator.aggregateDF(HiveDFTransformer.transform(auditRows.toDF()))
    ActualDataAndAuditAggregate(auditRows, auditAggregate)
  }


}
