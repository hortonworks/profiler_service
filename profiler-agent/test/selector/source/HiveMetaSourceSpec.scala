/*
 * HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
 * (c) 2016-2018 Hortonworks, Inc. All rights reserved.
 * This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
 * Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
 * to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
 * properly licensed third party, you do not have any rights to this code.
 * If this code is provided to you under the terms of the AGPLv3:
 * (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
 * (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
 *   LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
 * (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
 *   FROM OR RELATED TO THE CODE; AND
 * (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
 *   DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
 *   DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
 *   OR LOSS OR CORRUPTION OF DATA.
 */

package selector.source

import java.util

import job.selector.source.HiveMetastoreSource
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.scalatest.mockito.MockitoSugar
import org.scalatestplus.play.PlaySpec
import play.api.test.{DefaultAwaitTimeout, FutureAwaits}
import org.mockito.Mockito._

class HiveMetaSourceSpec extends PlaySpec with MockitoSugar with FutureAwaits with DefaultAwaitTimeout {

  "HiveMetastore Source" must {

    import scala.collection.JavaConverters._

    "get all tables in databases sequentially on each fetch next" in {
      val hiveClient = mock[HiveMetaStoreClient]

      when(hiveClient.getAllDatabases).thenReturn(List("db1", "db2", "db3").asJava)
      when(hiveClient.getAllTables("db1")).thenReturn(List("t11", "t12", "t13").asJava)
      when(hiveClient.getAllTables("db2")).thenReturn(List("t21", "t22").asJava)
      when(hiveClient.getAllTables("db3")).thenReturn(List("t31", "t32", "t33", "t34").asJava)

      val source = new HiveMetastoreSource(hiveClient)

      verify(hiveClient, times(1)).getAllDatabases

      val isDone1 = await(source.isDone())
      isDone1 mustBe false

      val tables1 = await(source.fetchNext())
      tables1.size mustBe 3
      tables1.head.id mustBe "db1.t11"
      (tables1.head.data \ "table").as[String] mustBe "t11"
      (tables1.head.data \ "db").as[String] mustBe "db1"
      tables1.last.id mustBe "db1.t13"

      verify(hiveClient, times(1)).getAllTables("db1")

      val isDone2 = await(source.isDone())
      isDone2 mustBe false

      val tables2 = await(source.fetchNext())
      tables2.size mustBe 2
      verify(hiveClient, times(1)).getAllTables("db2")

      val isDone3 = await(source.isDone())
      isDone3 mustBe false

      val tables3 = await(source.fetchNext())
      tables3.size mustBe 4
      verify(hiveClient, times(1)).getAllTables("db3")

      val isDone4 = await(source.isDone())
      isDone4 mustBe false

      val tables4 = await(source.fetchNext())
      tables4.isEmpty mustBe true

      val isDone5 = await(source.isDone())
      isDone5 mustBe true

    }

    "get no tables if no database are present" in {

      val hiveClient = mock[HiveMetaStoreClient]

      when(hiveClient.getAllDatabases).thenReturn(new util.ArrayList[String]())

      val source = new HiveMetastoreSource(hiveClient)
      verify(hiveClient, times(1)).getAllDatabases

      val isDone = await(source.isDone())
      isDone mustBe false

      val tables = await(source.fetchNext())
      tables.isEmpty mustBe true

      val isDone2 = await(source.isDone())
      isDone2 mustBe true

    }

    "get no tables for empty databases" in {

      val hiveClient = mock[HiveMetaStoreClient]

      when(hiveClient.getAllDatabases).thenReturn(List("db1", "db2", "db3").asJava)
      when(hiveClient.getAllTables("db1")).thenReturn(new util.ArrayList[String]())
      when(hiveClient.getAllTables("db2")).thenReturn(List("t21", "t22").asJava)
      when(hiveClient.getAllTables("db3")).thenReturn(List("t31", "t32", "t33", "t34").asJava)

      val source = new HiveMetastoreSource(hiveClient)

      verify(hiveClient, times(1)).getAllDatabases

      val isDone1 = await(source.isDone())
      isDone1 mustBe false

      val tables1 = await(source.fetchNext())
      tables1.isEmpty mustBe true

      verify(hiveClient, times(1)).getAllTables("db1")

      val isDone2 = await(source.isDone())
      isDone2 mustBe false

      val tables2 = await(source.fetchNext())
      tables2.size mustBe 2
      verify(hiveClient, times(1)).getAllTables("db2")

      val isDone3 = await(source.isDone())
      isDone3 mustBe false

      val tables3 = await(source.fetchNext())
      tables3.size mustBe 4
      verify(hiveClient, times(1)).getAllTables("db3")

      val isDone4 = await(source.isDone())
      isDone4 mustBe false

      val tables4 = await(source.fetchNext())
      tables4.isEmpty mustBe true

      val isDone5 = await(source.isDone())
      isDone5 mustBe true
    }

  }
}
