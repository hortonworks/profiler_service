package com.hortonworks.dataplane.profilers

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType, _}
import org.apache.spark.sql.{Row, SparkSession}

import scala.util.Random

case class RandomTable(database: String, table: String, count: Int, partitions: Int, provider: String, columns: Seq[(String, DataType)])


object RandomDataWarehouseBuilder {

  val random = new Random(System.currentTimeMillis())


  val availableTypes: List[DataType] = List(StringType, DoubleType, IntegerType, DateType, BooleanType)


  def generateTables(spark: SparkSession, numberOfTables: Int, databasesOpt: Option[List[String]] = None, tableSizeMin: Int = 10, tableSizeMax: Int = 30): Seq[RandomTable] = {
    (0 until numberOfTables).map(
      _ => {
        val database = databasesOpt match {
          case None =>
            val dbName = generateRandomName(5, 9)
            spark.sql(s"create database $dbName")
            dbName
          case Some(databases) => rnd(databases)
        }
        val tableName = getTableName(database, spark)
        val numberOfColumns = randomInt(5, 15)
        val columns = getColumns(Seq.empty[String], numberOfColumns)
        val typesOfColumns: Seq[(String, DataType)] = columns.map(x => (x, rnd(availableTypes)))
        val count = randomInt(tableSizeMin, tableSizeMax)
        val rows = (1 to count).map(_ => {
          Row.fromSeq(typesOfColumns.map(x => getValueOfType(x._2)))
        })
        val schema = StructType(typesOfColumns.map(x => StructField(x._1, x._2)))
        val rdd: RDD[Row] = spark.sparkContext.makeRDD(rows)
        val tableResultDF = spark.createDataFrame(rdd, schema)
        val partitionColumnAndNumberOfPartitions = if (random.nextBoolean()) {
          val partitionColumn: (String, Int) = scala.util.Random.shuffle(columns.zipWithIndex).head
          val partitions = rows.map(_.get(partitionColumn._2)).distinct.size
          List(partitionColumn._1) -> partitions
        }
        else
          List.empty -> 0

        val format: String = rnd(List("orc", "parquet"))

        tableResultDF.write.format(format).partitionBy(partitionColumnAndNumberOfPartitions._1: _*).saveAsTable(s"$database.$tableName")
        RandomTable(database, tableName, count, partitionColumnAndNumberOfPartitions._2, format, typesOfColumns)
      }
    )
  }


  private def getValueOfType(sparkDataType: DataType): Any = {
    sparkDataType match {
      case StringType =>
        generateRandomName(3, 40)
      case DoubleType =>
        random.nextDouble() * 1000
      case IntegerType =>
        randomInt(0, 10000)
      case DateType =>
        new java.sql.Date(System.currentTimeMillis() - random.nextInt(10000000))
      case BooleanType =>
        random.nextBoolean()
    }
  }


  private def rnd[A](items: List[A]): A = {
    items(random.nextInt(items.length))
  }

  private def getColumns(currentSeq: Seq[String], numberOfColumns: Int): Seq[String] = {
    if (currentSeq.length == numberOfColumns)
      currentSeq
    else {
      val columnName = generateRandomName(5, 9)
      if (currentSeq.contains(columnName))
        getColumns(currentSeq, numberOfColumns)
      else
        getColumns(currentSeq :+ columnName, numberOfColumns)
    }
  }

  private def getTableName(database: String, sparkSession: SparkSession): String = {
    val tableName = generateRandomName(5, 9)
    if (sparkSession.sqlContext.tableNames(database).contains(tableName))
      getTableName(database, sparkSession)
    else
      tableName
  }


  private def randomInt(minimum: Int, maximum: Int): Int = {
    random.nextInt(maximum - minimum) + minimum
  }


  protected def generateRandomName(minimumLength: Int, maximumLength: Int): String = {
    val availableCharacters = "abcdefghijklmnopqrstuvwxyz"

    val length = randomInt(minimumLength, maximumLength)
    (0 until length).map(_ =>
      availableCharacters.charAt(
        random.nextInt(availableCharacters.length)
      )
    ).mkString
  }


}
