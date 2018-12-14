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

package controllers

import javax.inject.{Inject, Singleton}

import domain.AssetSelectorDefinition
import domain.Error
import job.submitter.{ProfilerSubmitterManager, SubmitterRegistry}
import org.joda.time.DateTime
import play.api.libs.json.{JsError, JsObject, JsSuccess, Json}
import play.api.mvc.{Action, Controller}
import repo.AssetSelectorDefinitionRepo
import views.html._

import scala.concurrent.{ExecutionContext, Future}

@Singleton
class AssetSelectorDefinitions @Inject()(assetSelectorDefinitionRepo: AssetSelectorDefinitionRepo,
                                         submitterManager: ProfilerSubmitterManager)(implicit exec: ExecutionContext)
  extends Controller with JsonApi {

  import domain.JsonFormatters._

  def indexAction = Action {
    Ok(index.render())
  }

  def all = Action.async {
    assetSelectorDefinitionRepo.findAll().map(selectors => Ok(Json.toJson(selectors)))
      .recover(mapError)
  }

  def update = Action.async(parse.json) { req =>
    req.body
      .validate[AssetSelectorDefinition] match {
      case JsSuccess(selector, path) =>
        assetSelectorDefinitionRepo.findByName(selector.name).flatMap {
          case Some(sel) =>
            val selectorCopy = selector.copy(lastUpdated = DateTime.now())
            assetSelectorDefinitionRepo.update(selectorCopy).map {
              p =>
                submitterManager.removeSelectorByName(selectorCopy.name)
                submitterManager.addSelectorDefinition(selectorCopy)
                Ok(Json.toJson(selectorCopy))
            }
          case None =>
            Future.successful(NotFound(Json.toJson(Error("404", s"Selector ${selector.name} does not exist"))))
        }.recover(mapError)
      case e: JsError =>
        Future.successful(BadRequest(JsError.toFlatForm(e).toString()))

    }
  }

  def updateConfig(name: String, overwrite: Option[Boolean]) = Action.async(parse.json) { req =>
    req.body
      .validate[JsObject] match {
      case JsSuccess(config, path) =>
        assetSelectorDefinitionRepo.findByName(name).flatMap {
          case Some(sel) =>
            val newConfig = if(overwrite.getOrElse(false)) {
              config
            } else sel.config ++ config

            assetSelectorDefinitionRepo.updateConfig(name, newConfig).map {
              selector =>
                submitterManager.removeSelectorByName(selector.name)
                submitterManager.addSelectorDefinition(selector)
                Ok(Json.toJson(selector))
            }
          case None =>
            Future.successful(NotFound(Json.toJson(Error("404", s"Selector ${name} does not exist"))))
        }.recover(mapError)
      case e: JsError =>
        Future.successful(BadRequest(JsError.toFlatForm(e).toString()))

    }
  }

  def insert = Action.async(parse.json) { req =>
    req.body
      .validate[AssetSelectorDefinition] match {
      case JsSuccess(selector, path) =>
        assetSelectorDefinitionRepo.save(selector).map {
          p =>
            submitterManager.addSelectorDefinition(p)
            Ok(Json.toJson(p))
        }.recover(mapError)
      case e: JsError =>
        Future.successful(BadRequest(JsError.toFlatForm(e).toString()))

    }
  }
}
