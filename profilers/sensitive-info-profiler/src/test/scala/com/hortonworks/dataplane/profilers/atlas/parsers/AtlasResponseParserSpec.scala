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

import com.hortonworks.dataplane.profilers.atlas.models.{EntityResponse, ListAllTagResponse, SimpleEntityResponse}
import com.hortonworks.dataplane.profilers.atlas.parsers.AtlasResponseParsers._
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterAll, Matchers}
import play.api.libs.json.Json

import scala.io.Source

class AtlasResponseParserSpec extends AsyncFlatSpec with Matchers with BeforeAndAfterAll {


  private val simpleAtlasResponse: String = Source.fromInputStream(getClass.getResourceAsStream("/sample_simple_atlas_response.json")).mkString

  private val detailedAtlasResponse: String = Source.fromInputStream(getClass.getResourceAsStream("/sample_detailed_atlas_response.json")).mkString

  private val listTagResponse: String = Source.fromInputStream(getClass.getResourceAsStream("/sample_list_tag_response.json")).mkString

  "AtlasResponseParsers" should "parse Simple Atlas Response to SimpleEntityResponse" in {
    assert(Json.parse(simpleAtlasResponse).validate[SimpleEntityResponse] isSuccess)
  }

  "AtlasResponseParsers" should "parse Detailed Atlas Response to EntityResponse" in {
    assert(Json.parse(detailedAtlasResponse).validate[EntityResponse] isSuccess)
  }

  "AtlasResponseParsers" should "parse  tag list response to ListAllTagResponse" in {
    assert(Json.parse(listTagResponse).validate[ListAllTagResponse] isSuccess)
  }

}
