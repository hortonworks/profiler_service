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

package repo

import java.sql.Timestamp
import javax.inject.Singleton

import com.google.inject.Inject
import domain._
import org.joda.time.DateTime
import play.api.db.slick.DatabaseConfigProvider
import play.api.libs.json.{JsObject, Json}
import slick.jdbc.JdbcProfile

@Singleton
class AssetSelectorDefinitionRepo @Inject()(protected val dbConfigProvider: ProfilerDatabaseConfigProvider) {

  val dbConfig = dbConfigProvider.dbConfig
  val db = dbConfig.db

  import dbConfig.profile.api._
  import JsonFormatters._
  import scala.concurrent.ExecutionContext.Implicits.global

  val AssetSelectorDefinitions = TableQuery[AssetSelectorDefinitionsTable]

  def findAll() = db.run(AssetSelectorDefinitions.to[List].result)

  def findByProfilerInstane(instanceName: String) = db.run(
    AssetSelectorDefinitions.filter(_.profilerInstanceName === instanceName).to[List].result
  )

  def save(selectorDefinition: AssetSelectorDefinition) =
    db.run {
      (AssetSelectorDefinitions returning AssetSelectorDefinitions.map(_.id) into ((selectorDefinition, id) => selectorDefinition.copy(id = id))) += selectorDefinition
    }

  def findByName(name: String) = db.run {
    AssetSelectorDefinitions.filter(_.name === name).to[List].result.headOption
  }


  def update(selectorDefinition: AssetSelectorDefinition) =
    db.run {
      (AssetSelectorDefinitions.filter(_.name === selectorDefinition.name).update(selectorDefinition))
    }

  def updateSourceLastSubmitted(name: String, updateTimeMs: Long) = {
    db.run {
      AssetSelectorDefinitions.filter(_.name === name).map(_.sourceLastSubmitted).update(Some(updateTimeMs))
    }
  }

  implicit val jsObjColumnType = MappedColumnType.base[JsObject, String](
    json => json.toString(),
    s => Json.parse(s).as[JsObject]
  )

  implicit val sourceDefinitionColumnType = MappedColumnType.base[AssetSourceDefinition, String](
    e => Json.toJson(e).toString(),
    s => Json.parse(s).as[AssetSourceDefinition]
  )

  implicit val filterColumnType = MappedColumnType.base[Seq[AssetFilterDefinition], String](
    e => Json.toJson(e).toString,
    s => Json.parse(s).as[Seq[AssetFilterDefinition]]
  )

  implicit val localDateToDate = MappedColumnType.base[DateTime, Timestamp](
    l => new Timestamp(l.getMillis),
    d => new DateTime(d)
  )

  def updateConfig(name: String, config: JsObject) =
    db.run {
      (AssetSelectorDefinitions.filter(_.name === name).map(_.config).update(config))
    }.flatMap(e => db.run(AssetSelectorDefinitions.filter(_.name === name).to[List].result.head))

  final class AssetSelectorDefinitionsTable(tag: Tag) extends Table[AssetSelectorDefinition](tag, Some("profileragent"), "asset_selector_definitions") {

    def id = column[Option[Long]]("id", O.PrimaryKey, O.AutoInc)

    def name = column[String]("name")

    def profilerInstanceName = column[String]("profiler_instance_name")

    def selectorType = column[String]("selector_type")

    def config = column[JsObject]("config")

    def sourceDefinition = column[AssetSourceDefinition]("source_definition")

    def pickFilters = column[Seq[AssetFilterDefinition]]("pick_filters")

    def dropFilters = column[Seq[AssetFilterDefinition]]("drop_filters")

    def lastUpdated = column[DateTime]("lastupdated")

    def sourceLastSubmitted = column[Option[Long]]("source_lastsubmitted")

    override def * = (id, name, profilerInstanceName, selectorType, config, sourceDefinition, pickFilters, dropFilters, lastUpdated, sourceLastSubmitted) <> ((AssetSelectorDefinition.apply _) tupled, AssetSelectorDefinition.unapply)
  }

}
