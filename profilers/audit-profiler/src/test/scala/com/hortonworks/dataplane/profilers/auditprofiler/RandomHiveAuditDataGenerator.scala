package com.hortonworks.dataplane.profilers.auditprofiler

import scala.util.Random


case class AuditRow(reqUser: String, access: String, resource: String, action: String,
                    result: Long, repoType: Long = RandomHiveAuditDataGenerator.randomInt(10, 100).toLong,
                    resType: String = "@column", repo: String = "cl1_hive", logType: String = "RangerAudit",
                    cluster_name: String = "cl1", cliType: String = "HIVESERVER2")

object RandomHiveAuditDataGenerator {

  val accesses: List[String] = List("select", "delete", "create", "alter", "write")
  val action: List[String] = List("read", "write")

  val columnList: String = "cl1,cl2,cl3,cl4"

  val random = new Random(System.currentTimeMillis())


  def generateRandomAuditData(numberOfEvents: Int, numberOfTables: Int = randomInt(20, 80)): Seq[AuditRow] = {
    val databases: Seq[String] = getShortStrings(List(numberOfTables / 3, 2).max)
    val tables: Seq[String] = getShortStrings(numberOfTables)
    val users: Seq[String] = getShortStrings(10)
    (0 until numberOfEvents).map(
      _ => {
        AuditRow(rnd(users), rnd(accesses),
          s"${rnd(databases)}/${rnd(tables)}/$columnList",
          rnd(action), randomInt(0, 2)
        )
      }
    )
  }

  def rnd[A](items: Seq[A]): A = {
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

  private def getShortStrings(numberfTablesToAdd: Int, stringAccumulator: Seq[String] = Seq.empty[String]): Seq[String] = {
    if (numberfTablesToAdd == 0)
      stringAccumulator
    else {
      val shortString = generateRandomName(5, 9)
      if (stringAccumulator.contains(shortString))
        getShortStrings(numberfTablesToAdd, stringAccumulator)
      else getShortStrings(numberfTablesToAdd - 1, stringAccumulator :+ shortString)
    }
  }


  def randomInt(minimum: Int, maximum: Int): Int = {
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
