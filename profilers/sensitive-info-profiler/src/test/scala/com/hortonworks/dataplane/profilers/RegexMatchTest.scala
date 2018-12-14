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
package com.hortonworks.dataplane.profilers

import com.hortonworks.dataplane.profilers.kraptr.common.KraptrContext
import com.hortonworks.dataplane.profilers.kraptr.dsl.TagCreator
import com.hortonworks.dataplane.profilers.kraptr.engine.KraptrEngine
import com.hortonworks.dataplane.profilers.kraptr.models.dsl.MatchType
import com.hortonworks.dataplane.profilers.kraptr.vfs.hdfs.HDFSFileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSpec}

class RegexMatchTest extends FunSpec with BeforeAndAfterAll {


  private val resourcePath: String = getClass.getResource("/dsl/").getPath

  implicit val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()

  private val profilerInstanceName = "sensitiveinfo"
  val context = KraptrContext(resourcePath, profilerInstanceName,
    HDFSFileSystem(FileSystem.get(new Configuration())), spark, None, "kraptr2")


  private val tagCreatorsAndSchema: List[TagCreator] = KraptrEngine.buildTagCreators(context).get


  def validateCorrectMatching(value: String, label: String): Unit = {
    it(" matches the valid value " + value) {
      val decider = tagCreatorsAndSchema.filter(x => x.matchType == MatchType.value && x.tags.contains(label)).head.decider
      assert(decider.process(value))
    }
  }


  def validateCorrectNonMatching(value: String, label: String): Unit = {
    it(" doesn't match the invalid value " + value) {
      val decider = tagCreatorsAndSchema.filter(x => x.matchType == MatchType.value && x.tags.contains(label)).head.decider
      assert(!decider.process(value))
    }
  }


  describe("Credit card regex") {
    val validCCNumberList = List("5499180893080110", "4556354271809940")
    val invalidCCNumberList = List("123we", "$%%DFGDF")
    validCCNumberList.foreach(value => validateCorrectMatching(value, "creditcard"))
    invalidCCNumberList.foreach(value => validateCorrectNonMatching(value, "creditcard"))
  }

  describe("Email ID regex") {
    val validEmailList = List("nancyjpowers@jourrapide.com", "visharma@hortonworks.com")
    val invalidEmailList = List("ksfnkfn#$%$", "@djffdjf.ma")
    validEmailList.foreach(value => validateCorrectMatching(value, "email"))
    invalidEmailList.foreach(value => validateCorrectNonMatching(value, "email"))
  }

  describe("Name regex") {
    val validNameList = List("John", "Amir")
    val invalidNameList = List("3482y3", "sekfnsr%")
    validNameList.foreach(value => validateCorrectMatching(value, "name"))
    invalidNameList.foreach(value => validateCorrectNonMatching(value, "name"))
  }

  describe("Telephone number regex") {
    val validTelephoneList = List("856-232-9702", "442071838750")
    val invalidTelephoneList = List("$2323ksmd", "4325")
    validTelephoneList.foreach(value => validateCorrectMatching(value, "telephone"))
    invalidTelephoneList.foreach(value => validateCorrectNonMatching(value, "telephone"))
  }

  describe("IP Address regex") {
    val validIPs = List("172.16.254.1", "127.0.0.1")
    val invalidIPs = List("343.43.12.43", "$%232")
    validIPs.foreach(value => validateCorrectMatching(value, "ipaddress"))
    invalidIPs.foreach(value => validateCorrectNonMatching(value, "ipaddress"))
  }

  describe("SSN regex") {
    val validSSNs = List("427-43-1234", "711-21-5436")
    val invalidSSNs = List("000-12-5432", "912-12-4321", "321-432-2354", "7832-12-4325", "124-43-334534")
    validSSNs.foreach(value => validateCorrectMatching(value, "ssn"))
    invalidSSNs.foreach(value => validateCorrectNonMatching(value, "ssn"))
  }

  // IBAN Regex test cases

  describe("Germany IBAN ") {
    val validIBAN = List("DE89370400440532013000")
    val invalidIBAN = List("BE89370400440532013")
    validIBAN.foreach(value => validateCorrectMatching(value, "DEU_IBAN_Detection"))
    invalidIBAN.foreach(value => validateCorrectNonMatching(value, "DEU_IBAN_Detection"))
  }

  describe("United Kingdom IBAN ") {
    val validIBAN = List("GB29NWBK60161331926819")
    val invalidIBAN = List("ZZ89370400440532013")
    validIBAN.foreach(value => validateCorrectMatching(value, "GBR_IBAN_Detection"))
    invalidIBAN.foreach(value => validateCorrectNonMatching(value, "GBR_IBAN_Detection"))
  }

  describe("Latvia IBAN ") {
    val validIBAN = List("LV80BANK0000435195001")
    val invalidIBAN = List("BE89370400440532013")
    validIBAN.foreach(value => validateCorrectMatching(value, "LVA_IBAN_Detection"))
    invalidIBAN.foreach(value => validateCorrectNonMatching(value, "LVA_IBAN_Detection"))
  }

  describe("Finland IBAN ") {
    val validIBAN = List("FI2112345600000785")
    val invalidIBAN = List("BE89370400440532013")
    validIBAN.foreach(value => validateCorrectMatching(value, "FIN_IBAN_Detection"))
    invalidIBAN.foreach(value => validateCorrectNonMatching(value, "FIN_IBAN_Detection"))
  }

  describe("Estonia IBAN ") {
    val validIBAN = List("EE382200221020145685")
    val invalidIBAN = List("BE89370400440532013")
    validIBAN.foreach(value => validateCorrectMatching(value, "EST_IBAN_Detection"))
    invalidIBAN.foreach(value => validateCorrectNonMatching(value, "EST_IBAN_Detection"))
  }

  describe("Sweden IBAN ") {
    val validIBAN = List("SE4550000000058398257466")
    val invalidIBAN = List("BE89370400440532013")
    validIBAN.foreach(value => validateCorrectMatching(value, "SWE_IBAN_Detection"))
    invalidIBAN.foreach(value => validateCorrectNonMatching(value, "SWE_IBAN_Detection"))
  }

  describe("Norway IBAN ") {
    val validIBAN = List("NO9386011117947")
    val invalidIBAN = List("BE89370400440532013")
    validIBAN.foreach(value => validateCorrectMatching(value, "NOR_IBAN_Detection"))
    invalidIBAN.foreach(value => validateCorrectNonMatching(value, "NOR_IBAN_Detection"))
  }

  describe("Switzerland IBAN ") {
    val validIBAN = List("CH9300762011623852957")
    val invalidIBAN = List("BE89370400440532013")
    validIBAN.foreach(value => validateCorrectMatching(value, "CHE_IBAN_Detection"))
    invalidIBAN.foreach(value => validateCorrectNonMatching(value, "CHE_IBAN_Detection"))
  }

  describe("Bulgaria IBAN ") {
    val validIBAN = List("BG80BNBG96611020345678")
    val invalidIBAN = List("BE89370400440532013")
    validIBAN.foreach(value => validateCorrectMatching(value, "BGR_IBAN_Detection"))
    invalidIBAN.foreach(value => validateCorrectNonMatching(value, "BGR_IBAN_Detection"))
  }

  describe("Poland IBAN ") {
    val validIBAN = List("PL61109010140000071219812874")
    val invalidIBAN = List("BE89370400440532013")
    validIBAN.foreach(value => validateCorrectMatching(value, "POL_IBAN_Detection"))
    invalidIBAN.foreach(value => validateCorrectNonMatching(value, "POL_IBAN_Detection"))
  }

  describe("Ireland IBAN ") {
    val validIBAN = List("IE29AIBK93115212345678")
    val invalidIBAN = List("BE89370400440532013")
    validIBAN.foreach(value => validateCorrectMatching(value, "IRL_IBAN_Detection"))
    invalidIBAN.foreach(value => validateCorrectNonMatching(value, "IRL_IBAN_Detection"))
  }

  describe("Lithuania IBAN ") {
    val validIBAN = List("LT121000011101001000")
    val invalidIBAN = List("BE89370400440532013")
    validIBAN.foreach(value => validateCorrectMatching(value, "LTU_IBAN_Detection"))
    invalidIBAN.foreach(value => validateCorrectNonMatching(value, "LTU_IBAN_Detection"))
  }

  describe("Slovenia IBAN ") {
    val validIBAN = List("SI56263300012039086")
    val invalidIBAN = List("BE89370400440532013")
    validIBAN.foreach(value => validateCorrectMatching(value, "SVN_IBAN_Detection"))
    invalidIBAN.foreach(value => validateCorrectNonMatching(value, "SVN_IBAN_Detection"))
  }

  describe("Denmark IBAN ") {
    val validIBAN = List("DK5000400440116243")
    val invalidIBAN = List("BE89370400440532013")
    validIBAN.foreach(value => validateCorrectMatching(value, "DNK_IBAN_Detection"))
    invalidIBAN.foreach(value => validateCorrectNonMatching(value, "DNK_IBAN_Detection"))
  }

  describe("Slovakia IBAN ") {
    val validIBAN = List("SK3112000000198742637541")
    val invalidIBAN = List("BE89370400440532013")
    validIBAN.foreach(value => validateCorrectMatching(value, "SVK_IBAN_Detection"))
    invalidIBAN.foreach(value => validateCorrectNonMatching(value, "SVK_IBAN_Detection"))
  }

  describe("Malta IBAN ") {
    val validIBAN = List("MT84MALT011000012345MTLCAST001S")
    val invalidIBAN = List("BE89370400440532013")
    validIBAN.foreach(value => validateCorrectMatching(value, "MLT_IBAN_Detection"))
    invalidIBAN.foreach(value => validateCorrectNonMatching(value, "MLT_IBAN_Detection"))
  }

  describe("Luxembourg IBAN ") {
    val validIBAN = List("LU280019400644750000")
    val invalidIBAN = List("BE89370400440532013")
    validIBAN.foreach(value => validateCorrectMatching(value, "LUX_IBAN_Detection"))
    invalidIBAN.foreach(value => validateCorrectNonMatching(value, "LUX_IBAN_Detection"))
  }

  describe("Liechtenstein IBAN ") {
    val validIBAN = List("LI21088100002324013AA")
    val invalidIBAN = List("BE89370400440532013")
    validIBAN.foreach(value => validateCorrectMatching(value, "LIE_IBAN_Detection"))
    invalidIBAN.foreach(value => validateCorrectNonMatching(value, "LIE_IBAN_Detection"))
  }

  describe("France IBAN ") {
    val validIBAN = List("FR1420041010050500013M02606")
    val invalidIBAN = List("BE89370400440532013")
    validIBAN.foreach(value => validateCorrectMatching(value, "FRA_IBAN_Detection"))
    invalidIBAN.foreach(value => validateCorrectNonMatching(value, "FRA_IBAN_Detection"))
  }

  describe("Hungary IBAN ") {
    val validIBAN = List("HU42117730161111101800000000")
    val invalidIBAN = List("BE89370400440532013")
    validIBAN.foreach(value => validateCorrectMatching(value, "HUN_IBAN_Detection"))
    invalidIBAN.foreach(value => validateCorrectNonMatching(value, "HUN_IBAN_Detection"))
  }

  describe("Austria IBAN ") {
    val validIBAN = List("AT611904300234573201")
    val invalidIBAN = List("BE89370400440532013")
    validIBAN.foreach(value => validateCorrectMatching(value, "AUT_IBAN_Detection"))
    invalidIBAN.foreach(value => validateCorrectNonMatching(value, "AUT_IBAN_Detection"))
  }

  describe("Italy IBAN ") {
    val validIBAN = List("IT60X0542811101000000123456")
    val invalidIBAN = List("BE89370400440532013")
    validIBAN.foreach(value => validateCorrectMatching(value, "ITA_IBAN_Detection"))
    invalidIBAN.foreach(value => validateCorrectNonMatching(value, "ITA_IBAN_Detection"))
  }

  describe("Cyprus IBAN ") {
    val validIBAN = List("CY17002001280000001200527600")
    val invalidIBAN = List("BE89370400440532013")
    validIBAN.foreach(value => validateCorrectMatching(value, "CYP_IBAN_Detection"))
    invalidIBAN.foreach(value => validateCorrectNonMatching(value, "CYP_IBAN_Detection"))
  }

  describe("Iceland IBAN ") {
    val validIBAN = List("IS140159260076545510730339")
    val invalidIBAN = List("BE89370400440532013")
    validIBAN.foreach(value => validateCorrectMatching(value, "ISL_IBAN_Detection"))
    invalidIBAN.foreach(value => validateCorrectNonMatching(value, "ISL_IBAN_Detection"))
  }

  describe("Croatia IBAN ") {
    val validIBAN = List("HR1210010051863000160")
    val invalidIBAN = List("BE89370400440532013")
    validIBAN.foreach(value => validateCorrectMatching(value, "HRV_IBAN_Detection"))
    invalidIBAN.foreach(value => validateCorrectNonMatching(value, "HRV_IBAN_Detection"))
  }

  describe("Czech Republic IBAN ") {
    val validIBAN = List("CZ6508000000192000145399")
    val invalidIBAN = List("BE89370400440532013")
    validIBAN.foreach(value => validateCorrectMatching(value, "CZE_IBAN_Detection"))
    invalidIBAN.foreach(value => validateCorrectNonMatching(value, "CZE_IBAN_Detection"))
  }

  describe("Spain IBAN ") {
    val validIBAN = List("ES9121000418450200051332")
    val invalidIBAN = List("BE89370400440532013")
    validIBAN.foreach(value => validateCorrectMatching(value, "ESP_IBAN_Detection"))
    invalidIBAN.foreach(value => validateCorrectNonMatching(value, "ESP_IBAN_Detection"))
  }

  describe("Greece IBAN ") {
    val validIBAN = List("GR1601101250000000012300695")
    val invalidIBAN = List("BE89370400440532013")
    validIBAN.foreach(value => validateCorrectMatching(value, "GRC_IBAN_Detection"))
    invalidIBAN.foreach(value => validateCorrectNonMatching(value, "GRC_IBAN_Detection"))
  }

  describe("Netherlands IBAN ") {
    val validIBAN = List("NL91ABNA0417164300")
    val invalidIBAN = List("BE89370400440532013")
    validIBAN.foreach(value => validateCorrectMatching(value, "NLD_IBAN_Detection"))
    invalidIBAN.foreach(value => validateCorrectNonMatching(value, "NLD_IBAN_Detection"))
  }

  describe("Romania IBAN ") {
    val validIBAN = List("RO49AAAA1B31007593840000")
    val invalidIBAN = List("BE89370400440532013")
    validIBAN.foreach(value => validateCorrectMatching(value, "ROU_IBAN_Detection"))
    invalidIBAN.foreach(value => validateCorrectNonMatching(value, "ROU_IBAN_Detection"))
  }

  describe("Belgium IBAN ") {
    val validIBAN = List("BE68539007547034")
    val invalidIBAN = List("BE89370400440532013")
    validIBAN.foreach(value => validateCorrectMatching(value, "BEL_IBAN_Detection"))
    invalidIBAN.foreach(value => validateCorrectNonMatching(value, "BEL_IBAN_Detection"))
  }

  describe("IBAN ") {
    val validIBAN = List("NL91ABNA0417164300", "RO49AAAA1B31007593840000", "GR1601101250000000012300695", "ES9121000418450200051332")
    val invalidIBAN = List("BE89370400440532013", "XX12334ABCD")
    validIBAN.foreach(value => validateCorrectMatching(value, "IBAN_Detection"))
    invalidIBAN.foreach(value => validateCorrectNonMatching(value, "IBAN_Detection"))
  }

  // Passport regex test cases

  describe("Ireland Passport Number ") {
    val validPassportNumber = List("XN5004216")
    val invalidPassportNumber = List("LN500432P")
    validPassportNumber.foreach(value => validateCorrectMatching(value, "IRL_Passport_Detection"))
    invalidPassportNumber.foreach(value => validateCorrectNonMatching(value, "IRL_Passport_Detection"))
  }

  describe("Finland Passport Number ") {
    val validPassportNumber = List("XP8271602")
    val invalidPassportNumber = List("LN500432P")
    validPassportNumber.foreach(value => validateCorrectMatching(value, "FIN_Passport_Detection"))
    invalidPassportNumber.foreach(value => validateCorrectNonMatching(value, "FIN_Passport_Detection"))
  }

  describe("Austria Passport Number ") {
    val validPassportNumber = List("P4366918")
    val invalidPassportNumber = List("LN500432P")
    validPassportNumber.foreach(value => validateCorrectMatching(value, "AUT_Passport_Detection"))
    invalidPassportNumber.foreach(value => validateCorrectNonMatching(value, "AUT_Passport_Detection"))
  }

  describe("Italy Passport Number ") {
    val validPassportNumber = List("YA0000001")
    val invalidPassportNumber = List("LN500432P")
    validPassportNumber.foreach(value => validateCorrectMatching(value, "ITA_Passport_Detection"))
    invalidPassportNumber.foreach(value => validateCorrectNonMatching(value, "ITA_Passport_Detection"))
  }

  describe("Poland Passport Number ") {
    val validPassportNumber = List("ZS0000177")
    val invalidPassportNumber = List("LN500432P")
    validPassportNumber.foreach(value => validateCorrectMatching(value, "POL_Passport_Detection"))
    invalidPassportNumber.foreach(value => validateCorrectNonMatching(value, "POL_Passport_Detection"))
  }

  describe("Greece Passport Number ") {
    val validPassportNumber = List("AE0000005")
    val invalidPassportNumber = List("LN500432P")
    validPassportNumber.foreach(value => validateCorrectMatching(value, "GRC_Passport_Detection"))
    invalidPassportNumber.foreach(value => validateCorrectNonMatching(value, "GRC_Passport_Detection"))
  }

  describe("Germany Passport Number ") {
    val validPassportNumber = List("C01X00T47")
    val invalidPassportNumber = List("LN500432PA")
    validPassportNumber.foreach(value => validateCorrectMatching(value, "DEU_Passport_Detection"))
    invalidPassportNumber.foreach(value => validateCorrectNonMatching(value, "DEU_Passport_Detection"))
  }

  describe("Spain Passport Number ") {
    val validPassportNumber = List("AF238143")
    val invalidPassportNumber = List("LN500432P")
    validPassportNumber.foreach(value => validateCorrectMatching(value, "ESP_Passport_Detection"))
    invalidPassportNumber.foreach(value => validateCorrectNonMatching(value, "ESP_Passport_Detection"))
  }

  describe("Switzerland Passport Number ") {
    val validPassportNumber = List("S0002568")
    val invalidPassportNumber = List("LN500432P")
    validPassportNumber.foreach(value => validateCorrectMatching(value, "CHE_Passport_Detection"))
    invalidPassportNumber.foreach(value => validateCorrectNonMatching(value, "CHE_Passport_Detection"))
  }

  describe("France Passport Number ") {
    val validPassportNumber = List("05CK02237")
    val invalidPassportNumber = List("LN500432P")
    validPassportNumber.foreach(value => validateCorrectMatching(value, "FRA_Passport_Detection"))
    invalidPassportNumber.foreach(value => validateCorrectNonMatching(value, "FRA_Passport_Detection"))
  }

  describe("Belgium Passport Number ") {
    val validPassportNumber = List("EH000001")
    val invalidPassportNumber = List("LN500432P")
    validPassportNumber.foreach(value => validateCorrectMatching(value, "BEL_Passport_Detection"))
    invalidPassportNumber.foreach(value => validateCorrectNonMatching(value, "BEL_Passport_Detection"))
  }

  describe("Passport Number ") {
    val validPassportNumber = List("EH000001", "05CK02237", "C01X00T47", "AE0000005")
    val invalidPassportNumber = List("LN500432PA2")
    validPassportNumber.foreach(value => validateCorrectMatching(value, "Passport_Detection"))
    invalidPassportNumber.foreach(value => validateCorrectNonMatching(value, "Passport_Detection"))
  }

  // National Id regex test cases

  describe("Norway National Id ") {
    val validNationalId = List("29029900157")
    val invalidNationalId = List("89029900157")
    validNationalId.foreach(value => validateCorrectMatching(value, "NOR_NationalID_Detection"))
    invalidNationalId.foreach(value => validateCorrectNonMatching(value, "NOR_NationalID_Detection"))
  }

  describe("Czech Republic National Id ") {
    val validNationalId = List("7103192745")
    val invalidNationalId = List("590312123")
    validNationalId.foreach(value => validateCorrectMatching(value, "CZE_NationalID_Detection"))
    invalidNationalId.foreach(value => validateCorrectNonMatching(value, "CZE_NationalID_Detection"))
  }

  describe("Sweden National Id ") {
    val validNationalId = List("811228-9874")
    val invalidNationalId = List("811328-9873")
    validNationalId.foreach(value => validateCorrectMatching(value, "SWE_NationalID_Detection"))
    invalidNationalId.foreach(value => validateCorrectNonMatching(value, "SWE_NationalID_Detection"))
  }

  describe("Bulgaria National Id ") {
    val validNationalId = List("7523169263", "8032056031")
    val invalidNationalId = List("8019010008")
    validNationalId.foreach(value => validateCorrectMatching(value, "BGR_NationalID_Detection"))
    invalidNationalId.foreach(value => validateCorrectNonMatching(value, "BGR_NationalID_Detection"))
  }

  describe("Slovakia National Id ") {
    val validNationalId = List("710319/2745")
    val invalidNationalId = List("710332/2746")
    validNationalId.foreach(value => validateCorrectMatching(value, "SVK_NationalID_Detection"))
    invalidNationalId.foreach(value => validateCorrectNonMatching(value, "SVK_NationalID_Detection"))
  }

  describe("Denmark National Id ") {
    val validNationalId = List("211062-5629")
    val invalidNationalId = List("511062-5629")
    validNationalId.foreach(value => validateCorrectMatching(value, "DNK_NationalID_Detection"))
    invalidNationalId.foreach(value => validateCorrectNonMatching(value, "DNK_NationalID_Detection"))
  }

  describe("Latvia National Id ") {
    val validNationalId = List("161175-19997")
    val invalidNationalId = List("161375-19997")
    validNationalId.foreach(value => validateCorrectMatching(value, "LVA_NationalID_Detection"))
    invalidNationalId.foreach(value => validateCorrectNonMatching(value, "LVA_NationalID_Detection"))
  }

  describe("Portugal National Id ") {
    val validNationalId = List("00000000-0ZZ4")
    val invalidNationalId = List("00000000-AZZ4")
    validNationalId.foreach(value => validateCorrectMatching(value, "PRT_NationalID_Detection"))
    invalidNationalId.foreach(value => validateCorrectNonMatching(value, "PRT_NationalID_Detection"))
  }

  describe("Estonia National Id ") {
    val validNationalId = List("37605030299")
    val invalidNationalId = List("376050302999")
    validNationalId.foreach(value => validateCorrectMatching(value, "EST_NationalID_Detection"))
    invalidNationalId.foreach(value => validateCorrectNonMatching(value, "EST_NationalID_Detection"))
  }

  describe("Poland National Id ") {
    val validNationalId = List("83010411457", "87123116221")
    val invalidNationalId = List("04271113861")
    validNationalId.foreach(value => validateCorrectMatching(value, "POL_NationalID_Detection"))
    invalidNationalId.foreach(value => validateCorrectNonMatching(value, "POL_NationalID_Detection"))
  }

  describe("Iceland National Id ") {
    val validNationalId = List("120174-3399")
    val invalidNationalId = List("1201743397")
    validNationalId.foreach(value => validateCorrectMatching(value, "ISL_NationalID_Detection"))
    invalidNationalId.foreach(value => validateCorrectNonMatching(value, "ISL_NationalID_Detection"))
  }

  describe("Romania National Id ") {
    val validNationalId = List("1630615123457", "1800101221199")
    val invalidNationalId = List("1630615983458")
    validNationalId.foreach(value => validateCorrectMatching(value, "ROU_NationalID_Detection"))
    invalidNationalId.foreach(value => validateCorrectNonMatching(value, "ROU_NationalID_Detection"))
  }

  describe("Lithuania National Id ") {
    val validNationalId = List("38703181745")
    val invalidNationalId = List("38703421745")
    validNationalId.foreach(value => validateCorrectMatching(value, "LTU_NationalID_Detection"))
    invalidNationalId.foreach(value => validateCorrectNonMatching(value, "LTU_NationalID_Detection"))
  }

  describe("Switzerland National Id ") {
    val validNationalId = List("756.1234.5678.95")
    val invalidNationalId = List("856.1234.5678.95")
    validNationalId.foreach(value => validateCorrectMatching(value, "CHE_NationalID_Detection"))
    invalidNationalId.foreach(value => validateCorrectNonMatching(value, "CHE_NationalID_Detection"))
  }

  describe("Finland National Id ") {
    val validNationalId = List("311280-888Y")
    val invalidNationalId = List("131052-001U")
    validNationalId.foreach(value => validateCorrectMatching(value, "FIN_NationalID_Detection"))
    invalidNationalId.foreach(value => validateCorrectNonMatching(value, "FIN_NationalID_Detection"))
  }

  describe("Spain National Id ") {
    val validNationalId = List("99999999R")
    val invalidNationalId = List("X-2482300A")
    validNationalId.foreach(value => validateCorrectMatching(value, "ESP_NationalID_Detection"))
    invalidNationalId.foreach(value => validateCorrectNonMatching(value, "ESP_NationalID_Detection"))
  }

  describe("Italy National Id ") {
    val validNationalId = List("MRTMTT25D09F205Z", "MLLSNT82P65Z404U")
    val invalidNationalId = List("4RTMTT25D09F205Z")
    validNationalId.foreach(value => validateCorrectMatching(value, "ITA_NationalID_Detection"))
    invalidNationalId.foreach(value => validateCorrectNonMatching(value, "ITA_NationalID_Detection"))
  }

  describe("National Id ") {
    val validNationalId = List("MRTMTT25D09F205Z", "99999999R", "38703181745", "1800101221199")
    val invalidNationalId = List("4RTMTT25D09F205Z", "X-2482300A", "38703421745", "1630615983458")
    validNationalId.foreach(value => validateCorrectMatching(value, "NationalID_Detection"))
    invalidNationalId.foreach(value => validateCorrectNonMatching(value, "NationalID_Detection"))
  }

}
