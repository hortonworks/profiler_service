package com.hortonworks.dataplane.profilers.worker

import com.hortonworks.dataplane.profilers.hdpinterface.spark.SimpleSpark
import com.hortonworks.dataplane.profilers.kraptr.behaviour.inbuilt.InbuiltBehaviours
import com.hortonworks.dataplane.profilers.kraptr.dsl.TagCreator
import com.hortonworks.dataplane.profilers.kraptr.models.dsl.MatchType
import org.apache.spark.sql.SparkSession

import scala.util.Random

case class TableRow(id: Int, ipaddress: String, age: Int, email: String, iban: String)

object RowsBuilder {

  val rand = new Random(System.currentTimeMillis())

  private def buildRowsOfSize(size: Int, ipaddresses: List[String], ages: List[Int], emails: List[String], ibans: List[String]): List[TableRow] = {
    (1 to size).map(
      id => {

        TableRow(id,
          rnd(ipaddresses),
          rnd(ages),
          rnd(emails),
          rnd(ibans)
        )
      }
    ).toList
  }

  def buildRowsAndFillBadData(numberOfGoodRows: Int, ipaddresses: List[String],
                              ages: List[Int], emails: List[String],
                              ibans: List[String], badRow: TableRow,
                              numberOfBadRows: Int): List[TableRow] = {
    val goodRows = buildRowsOfSize(numberOfGoodRows, ipaddresses, ages, emails, ibans)
    val badRows = ((numberOfGoodRows + 1) to (numberOfGoodRows + numberOfBadRows)).map(
      x => badRow.copy(id = x)
    ).toList
    goodRows ::: badRows
  }


  private def rnd[A](items: List[A]): A = {
    items(rand.nextInt(items.length))
  }

}

object DefaultInventories {

  implicit val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()

  val interface: SimpleSpark = new SimpleSpark(spark)

  val inbuilt: InbuiltBehaviours = new InbuiltBehaviours()

  def dslWithModifiedConfidences(ipaddressWeights: (Int, Int) = (15, 85),
                                 emailWegihts: (Int, Int) = (15, 85),
                                 ibanWeights: (Int, Int) = (15, 85)): List[TagCreator] = {
    List(
      new TagCreator(inbuilt.is_in_ci("ipaddress", "ipaddr", "ip"), List("ipaddress"), MatchType.name, ipaddressWeights._1),
      new TagCreator(inbuilt.is_in_ci("ipaddress", "ipaddr", "ip"), List("ipaddress"), MatchType.name, ipaddressWeights._1),
      new TagCreator(inbuilt.is_in_ci("age", "yearold"), List("age"), MatchType.name, 15),
      new TagCreator(inbuilt.is_in_ci("email", "mail"), List("email"), MatchType.name, emailWegihts._1),
      new TagCreator(inbuilt.is_in_ci("Sweden IBAN"
        , "Sweden International Bank Account Number"), List("SWE_IBAN_Detection"), MatchType.name, ibanWeights._1),
      new TagCreator(inbuilt.regex("\\b(((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\." +
        "(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\." +
        "(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)))\\b"), List("ipaddress"), MatchType.value, ipaddressWeights._2),
      new TagCreator(inbuilt.regex("\\b(((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\." +
        "(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\." +
        "(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)))\\b"), List("ipaddress"), MatchType.value, ipaddressWeights._2),
      new TagCreator(inbuilt.regex("\\b(([a-zA-Z0-9_\\-\\.]+)@((\\[[0-9]{1,3}\\.[0-9]{1,3}\\" +
        ".[0-9]{1,3}\\.)|(([a-zA-Z0-9\\-]+\\" +
        ".)+))([a-zA-Z]{2,4}|[0-9]{1,3})(\\]?))\\b"), List("email"), MatchType.value, emailWegihts._2),
      new TagCreator(inbuilt.regex("\\b(([a-zA-Z0-9_\\-\\.]+)@((\\[[0-9]{1,3}\\.[0-9]{1,3}\\" +
        ".[0-9]{1,3}\\.)|(([a-zA-Z0-9\\-]+\\" +
        ".)+))([a-zA-Z]{2,4}|[0-9]{1,3})(\\]?))\\b"), List("email2"), MatchType.value, emailWegihts._2),
      new TagCreator(inbuilt.regex("(\\s|,|;|\\A)(SE\\d{2}([ ]?)" +
        "\\d{4}\\3\\d{4}\\3\\d{4}\\3\\d{4}" +
        "\\3\\d{4})(\\s|,|;|\\Z)"), List("SWE_IBAN_Detection"), MatchType.value, ibanWeights._2)
    )
  }

}


