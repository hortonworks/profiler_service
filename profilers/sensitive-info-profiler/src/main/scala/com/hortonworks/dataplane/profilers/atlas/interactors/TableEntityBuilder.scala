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
package com.hortonworks.dataplane.profilers.atlas.interactors

import com.hortonworks.dataplane.profilers.atlas.models.CustomModels.{MinimalEntity, TableEntity, Tag}
import com.hortonworks.dataplane.profilers.atlas.models.{Classification, EntityResponse, TableResponseEntity}
import com.hortonworks.dataplane.profilers.commons.Constants

object TableEntityBuilder {

  val statusIdentifier = "status"

  def buildFrom(entityResponse: EntityResponse): List[TableEntity] = {
    val entitiesOfTypeHiveTable = entityResponse.entities.filter(isEntityAHiveTable)
    entitiesOfTypeHiveTable.map(
      tableEntity => {
        val minimalColumnEntities = buildMinimalColumnEntities(tableEntity, entityResponse)
        val tags = dpTagsFromClassifications(tableEntity.classifications)
        val minimalTableEntity = MinimalEntity(tableEntity.attributes.qualifiedName, tableEntity.guid, tags)
        TableEntity(minimalTableEntity, minimalColumnEntities)
      }
    )
  }

  private def buildMinimalColumnEntities(tableEntity: TableResponseEntity, entityResponse: EntityResponse) = {
    tableEntity.attributes.columns.map(
      column => {
        val columnEntity = entityResponse.referredEntities(column.guid)
        val classifications = columnEntity.classifications
        val tags = dpTagsFromClassifications(classifications)
        MinimalEntity(columnEntity.attributes.qualifiedName, columnEntity.guid, tags)
      }
    )
  }

  private def dpTagsFromClassifications(classifications: List[Classification]) = {
    classifications.filter(classification => {
      classification.typeName.startsWith(Constants.dpTagPrefix) && classification.attributes.contains(statusIdentifier)
    }).map(
      classification => {
        val actualTag: String = classification.typeName.replaceFirst(Constants.dpTagPrefix, "")
        Tag(actualTag, classification.attributes(statusIdentifier).as[String])
      }
    )
  }

  private def isEntityAHiveTable(entity: TableResponseEntity) = {
    entity.typeName == "hive_table"
  }

  private def isEntityAHiveColumn(entity: TableResponseEntity) = {
    entity.typeName == "hive_column"
  }
}
