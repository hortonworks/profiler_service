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

package com.hortonworks.dataplane.profilers.atlas

import java.nio.charset.StandardCharsets
import java.util.Base64

import com.hortonworks.dataplane.profilers.commons.Constants
import com.hortonworks.dataplane.profilers.commons.Models.AtlasInfo
import scalaj.http.{Http, HttpResponse}

import scala.util.{Failure, Success, Try}


class TagCreatorService(atlasInfo: AtlasInfo) {

  import org.json4s._
  import org.json4s.jackson.JsonMethods._

  private val atlasAuthString = "Basic " + Base64.getEncoder.encodeToString((atlasInfo.user + ":" +
    atlasInfo.password).getBytes(StandardCharsets.UTF_8))

  private val headers = List(("X-XSRF-HEADER", "valid"), ("Content-Type", "application/json"), ("Authorization", atlasAuthString))
  private val AtlasRESTEndpoint = s"${atlasInfo.url}/api/atlas/v2"


  private val ClassificationTagUrl = AtlasRESTEndpoint + "/types/classificationdef/name/"

  def createDPTagIfNotExists() = {
    dpTagExists() match {
      case Success(_) =>
        Success(s"Dp Tag Already Exists")
      case Failure(th) =>
        createTags(Seq(Constants.DPSuperTypeTag), List(), false)
    }
  }


  def createColumnTags(tags: Seq[String]) = {
    val modifiedTags = tags.map(Constants.dpTagPrefix + _)
    createTags(modifiedTags)
  }


  private def tryAtlasApi[T](apiCall: => HttpResponse[String], onSucces: String => T, successCodes: Seq[Int] = Seq(200)): Try[T] = Try {
    val resp = apiCall

    if (successCodes.contains(resp.code)) onSucces(resp.body)
    else throw new AtlasApiException(s"Code : ${resp.code}, Body : ${resp.body}")
  }

  private def dpTagExists(): Try[String] = tryAtlasApi(
    Http(s"$ClassificationTagUrl${Constants.DPSuperTypeTag}").headers(headers).asString,
    (st) => st
  )


  private def createTags(tagNames: Seq[String],
                         superTag: List[JString] = List(JString(Constants.DPSuperTypeTag)),
                         addStateAttributes: Boolean = true
                        ): Try[String] = if (tagNames.nonEmpty) {
    tryAtlasApi({
      val url = AtlasRESTEndpoint + "/types/typedefs"
      val attributeDefs = if (addStateAttributes) {
        JArray(List(
          JObject(
            JField("cardinality", JString("SINGLE")),
            JField("isIndexable", JBool(false)),
            JField("isOptional", JBool(false)),
            JField("isUnique", JBool(false)),
            JField("name", JString("status")),
            JField("sortKey", JString("status")),
            JField("typeName", JString("string")),
            JField("valuesMaxCount", JDouble(1)),
            JField("valuesMinCount", JDouble(0))
          )
        ))
      } else JArray(List())

      val classificationData = tagNames.map {
        t =>
          JObject(
            JField("name", JString(t)),
            JField("superTypes", JArray(superTag)),
            JField("attributeDefs", attributeDefs)
          )
      }.toList

      val emptyArray = JArray(List())

      val jsonData = JObject(
        JField("classificationDefs", JArray(classificationData)),
        JField("enumDefs", emptyArray),
        JField("structDefs", emptyArray),
        JField("entityDefs", emptyArray),
        JField("empty", JBool(true))
      )

      Http(url).headers(headers).postData(pretty(jsonData)).asString
    }, (t) => t)
  } else Success("No new tags to create")

}