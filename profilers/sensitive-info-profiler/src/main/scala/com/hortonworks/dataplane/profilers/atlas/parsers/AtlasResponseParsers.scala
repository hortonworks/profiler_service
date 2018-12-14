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
package com.hortonworks.dataplane.profilers.atlas.parsers

import com.hortonworks.dataplane.profilers.atlas.models._
import play.api.libs.json._

object AtlasResponseParsers {

  private val attributeIdentifier = "attributes"

  private val typeNameIdentifier = "typeName"

  private val allowedReferenceEntityTypes = Set("hive_column")


  implicit val classificationRead: Format[Classification] = new Format[Classification] {
    override def reads(json: JsValue): JsResult[Classification] = (json \ typeNameIdentifier).validate[String] flatMap (
      typeName => {
        val attributesResult = if (json.as[JsObject].keys.contains(attributeIdentifier))
          (json \ attributeIdentifier).validate[Map[String, JsValue]]
        else
          JsSuccess(Map.empty[String, JsValue])
        attributesResult.map(Classification(typeName, _))
      }
      )

    override def writes(classification: Classification): JsValue = {
      Json.writes[Classification].writes(classification)
    }
  }

  private implicit val entityAttributeFormat: Format[SimpleEntityAttributes] = Json.format[SimpleEntityAttributes]
  private implicit val simpleEntityFormat: Format[SimpleEntity] = Json.format[SimpleEntity]
  implicit val simpleEntityResponseFormat: Format[SimpleEntityResponse] = Json.format[SimpleEntityResponse]

  private implicit val columnFormat: Format[Column] = Json.format[Column]
  private implicit val tableEntityAttributeFormat: Format[TableEntityAttributes] = Json.format[TableEntityAttributes]
  private implicit val tableEntityFormat: Format[TableResponseEntity] = Json.format[TableResponseEntity]
  private implicit val columnEntityAttributeFormat: Format[ColumnEntityAttributes] = Json.format[ColumnEntityAttributes]
  private implicit val subResponseEntityFormat: Format[ResponseColumnEntity] = new Format[ResponseColumnEntity] {

    override def reads(json: JsValue): JsResult[ResponseColumnEntity] = {
      val typeName = (json \ "typeName").as[String]
      if (allowedReferenceEntityTypes.contains(typeName)) {
        Json.reads[ResponseColumnEntity].reads(json)
      }
      else {
        val guid = (json \ "guid").as[String]
        JsSuccess(ResponseColumnEntity(typeName, ColumnEntityAttributes("na", "na"), guid, "na", Some(List.empty)))
      }
    }

    override def writes(responseColumnEntity: ResponseColumnEntity): JsValue = Json.writes[ResponseColumnEntity].writes(responseColumnEntity)
  }
  implicit val entityResponseFormat: Format[EntityResponse] = Json.format[EntityResponse]


  private implicit val classificationDefinitionsFormat: Format[ClassificationDefinitions] = Json.format[ClassificationDefinitions]

  implicit val listAllTagResponseFormat: Format[ListAllTagResponse] = Json.format[ListAllTagResponse]
}
