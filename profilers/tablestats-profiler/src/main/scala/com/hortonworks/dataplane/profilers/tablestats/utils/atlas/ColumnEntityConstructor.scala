/*
 HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES


  (c) 2016-2018 Hortonworks, Inc. All rights reserved.

  This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
  Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
  to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
  properly licensed third party, you do not have any rights to this code.

  If this code is provided to you under the terms of the AGPLv3:
  (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
  (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
    LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
  (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
    FROM OR RELATED TO THE CODE; AND
  (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
    DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
    DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
    OR LOSS OR CORRUPTION OF DATA.
*/

package com.hortonworks.dataplane.profilers.tablestats.utils.atlas

import com.hortonworks.dataplane.profilers.commons.atlas.AtlasConstants
import com.typesafe.scalalogging.LazyLogging
import org.apache.atlas.typesystem.{Referenceable, Struct}

object ColumnEntityConstructor extends LazyLogging {

  def getColumnReferenceable(columnName: String, columnQualifiedName: String,
                             columnMetrics: Map[String, Any]): Referenceable = {
    val hiveColumnReferenceable = new Referenceable(AtlasConstants.HiveColumnTypeName)
    val columnDataType = columnMetrics("type")

    hiveColumnReferenceable.set(AtlasConstants.TypeAttribute, columnDataType)
    hiveColumnReferenceable.set(AtlasConstants.QualifiedName, columnQualifiedName)
    hiveColumnReferenceable.set(AtlasConstants.Name, columnName)

    val columnProfileData = new Struct(AtlasConstants.HiveColumnProfileData)
    columnMetrics.get("count").map(columnProfileData.set("nonNullCount", _))
    columnMetrics.get("distinct").map(columnProfileData.set("cardinality", _))

    columnDataType match {
      case "int" | "long" | "double" | "decimal" | "float" => {
        columnMetrics.get("min").map(columnProfileData.set("minValue", _))
        columnMetrics.get("max").map(columnProfileData.set("maxValue", _))
        columnMetrics.get("mean").map(columnProfileData.set("meanValue", _))
        columnMetrics.get("stddev").map(columnProfileData.set("stdDeviation", _))
        columnMetrics.get("histogram").map(columnProfileData.set("histogram", _))
        columnMetrics.get("fq").map(columnProfileData.set("frequentItems", _))
        columnMetrics.get("quartiles").map(columnProfileData.set("quartiles", _))
      }
      case "string" => {
        columnMetrics.get("min").map(columnProfileData.set("lengthMinValue", _))
        columnMetrics.get("max").map(columnProfileData.set("lengthMaxValue", _))
        columnMetrics.get("mean").map(columnProfileData.set("lengthMeanValue", _))
        columnMetrics.get("stddev").map(columnProfileData.set("lengthStdDeviation", _))
        columnMetrics.get("fqh").map(columnProfileData.set("histogram", _))
        columnMetrics.get("fq").map(columnProfileData.set("frequentItems", _))
      }

      case "boolean" => {
        columnMetrics.get("trueCount").map(columnProfileData.set("numTrue", _))
        columnMetrics.get("falseCount").map(columnProfileData.set("numFalse", _))
      }
      case _ =>
        logger.warn("Column type {} not supported for atlas", columnDataType.toString)
    }

    hiveColumnReferenceable.set(AtlasConstants.ProfileDataAttr, columnProfileData)
    hiveColumnReferenceable
  }
}
