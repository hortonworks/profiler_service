package com.hortonworks.dataplane.profilers.tablestats

import scala.util.Random

case class TableRow(id: Int, ipaddress: String, age: Int, email: String,
                    iban: String, date: java.sql.Date, isworking: Boolean)

object RowsBuilder {


  val rand = new Random(System.currentTimeMillis())

  def buildRowsOfSize(size: Int, ipaddresses: List[String],
                      ages: List[Int], emails: List[String],
                      ibans: List[String]): List[TableRow] = {
    (1 to size).map(
      id => {

        TableRow(id,
          rnd(ipaddresses),
          rnd(ages),
          rnd(emails),
          rnd(ibans),
          new java.sql.Date(System.currentTimeMillis() - rand.nextInt(10000000)),
          rand.nextBoolean()
        )
      }
    ).toList
  }


  private def rnd[A](items: List[A]): A = {
    items(rand.nextInt(items.length))
  }


}

object StatisticalFunctions {

  import Numeric.Implicits._

  def mean[T: Numeric](xs: Iterable[T]): Double = xs.sum.toDouble / xs.size

  def variance[T: Numeric](xs: Iterable[T]): Double = {
    val avg = mean(xs)

    xs.map(_.toDouble).map(a => math.pow(a - avg, 2)).sum / xs.size
  }

  def stdDev[T: Numeric](xs: Iterable[T]): Double = math.sqrt(variance(xs))
}



