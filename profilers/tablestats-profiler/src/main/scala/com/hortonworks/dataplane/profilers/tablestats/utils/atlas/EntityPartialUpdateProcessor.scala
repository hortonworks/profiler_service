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

import com.hortonworks.dataplane.profilers.commons.atlas.{AtlasConstants, AtlasInterface}
import com.typesafe.scalalogging.LazyLogging

import scalaj.http.HttpResponse
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

object EntityPartialUpdateProcessor extends LazyLogging{

  def updateEntityIfExistsInAtlas(qualifiedName:String, postCallObj:AtlasRestCall,
                                       getEntityResponse:Future[HttpResponse[String]]):Future[Unit] = {
    logger.info("Attempting to partially update the entity {}", qualifiedName)
    getEntityResponse.flatMap(entityResponse => entityResponse.code match {
      case 200 => {
        val entityPartialUpdateResponse = AtlasInterface.atlasApiHandler(postCallObj.urlList,
          AtlasConstants.atlasEntityPartialUpdateRESTEndpoint, postCallObj.headers,
          postCallObj.params, postCallObj.method, postCallObj.requestJson)
        entityPartialUpdateResponse.flatMap { partialUpdateResponse =>
          partialUpdateResponse.code match {
            case 200 => {
              logger.info("Profile data update for entity {} successful with response code {} and body {}",
                qualifiedName, partialUpdateResponse.code.toString, partialUpdateResponse.body)
            }
            case _ => {
              logger.warn("Profile data update for entity {} failed with response code {} and body {}",
                qualifiedName, partialUpdateResponse.code.toString, partialUpdateResponse.body)
            }
          }
          Future.successful()
        }
      }
      case 404 => {
        logger.debug("Entity with qualifiedName {} not found in Atlas. New entity will be registered ",
          qualifiedName)
        Future.successful()
      }
      case _ => {
        logger.error("Unexpected error while fetching details for entity {} from Atlas. Response code {}" +
          " body {}", qualifiedName, entityResponse.code.toString, entityResponse.body)
        Future.failed(new Exception("Unexpected error while fetching entity details from Atlas"))
      }
    })
  }
}
