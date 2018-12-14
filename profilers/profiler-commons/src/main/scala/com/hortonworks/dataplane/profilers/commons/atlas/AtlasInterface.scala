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
package com.hortonworks.dataplane.profilers.commons.atlas

import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.Future
import scalaj.http.{Http, HttpResponse}
import scala.concurrent.ExecutionContext.Implicits.global

object AtlasInterface extends LazyLogging {

  def tryAtlasApi(url: String, headers: List[(String, String)], params: List[(String, String)],
                  method: String = "GET", requestJSON: String = ""): Future[HttpResponse[String]] = {
    logger.debug("Attempting HTTP method {} on resource {}", method, url)
    //TODO - Configure Atlas connection and read timeouts in mpack
    method match {
      case "GET" => {
        Future {
          Http(url).timeout(connTimeoutMs = 300000, readTimeoutMs = 300000).headers(headers).
            params(params).asString
        }
      }
      case "POST" => {
        Future {
          Http(url).timeout(connTimeoutMs = 300000, readTimeoutMs = 300000).headers(headers).
            params(params).postData(requestJSON).asString
        }
      }
      case "PUT" => {
        Future {
          Http(url).timeout(connTimeoutMs = 300000, readTimeoutMs = 300000).headers(headers).
            params(params).postData(requestJSON).method("PUT").asString
        }
      }
      case _ => {
        logger.warn("HTTP {} Method not supported on resource {}", method, url)
        Future.failed(new Exception("Method not supported"))
      }
    }
  }

  private def isRedirectResponse(responseCode: Int): Boolean = {
    Set(302, 303, 307).contains(responseCode)
  }

  def atlasApiHandler(urlList: List[String], apiEndpoint: String, headers: List[(String, String)], params: List[(String, String)],
                      method: String = "GET", requestJSON: String = ""): Future[HttpResponse[String]] = {

    urlList match {
      case head :: tail => {
        val requestUrl = head + apiEndpoint
        val response = tryAtlasApi(requestUrl, headers, params, method, requestJSON)
        response flatMap { r =>
          if (isRedirectResponse(r.code)) {
            atlasApiHandler(tail, apiEndpoint, headers, params, method, requestJSON)
          }
          else if (r.code >= 300) {
            logger.warn(s"Call to Atlas :$requestUrl failed unexpectedly with code ${r.code} and response: ${r.body}" +
              s"; method: $method , query params: ${params.mkString(",")} , payload : $requestJSON")
            Future.failed(new Exception(s"API Request to Atlas server failed with code ${r.code} ,response: ${r.body}"))
          }
          else {
            Future.successful(r)
          }
        } recoverWith {
          case t: Exception => {
            logger.info("Atlas API request to URL {} failed with exception {}. Retrying other URLs", requestUrl, t)
            atlasApiHandler(tail, apiEndpoint, headers, params, method, requestJSON)
          }
        }
      }
      case _ => {
        logger.error("API Request to Atlas server failed")
        Future.failed(new Exception("API Request to Atlas server failed"))
      }
    }
  }
}