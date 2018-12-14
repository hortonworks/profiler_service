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
import domain._
import job.submitter.ProfilerSubmitterManager
import org.joda.time.DateTime
import play.api.Logger
import play.api.libs.json.{JsError, JsSuccess, Json}
import play.api.mvc.{Action, Controller}
import repo.{AssetSelectorDefinitionRepo, ProfilerInstanceRepo}
import utils.YarnInteractor
import utils.ProfilerConfigValidator
import views.html._

import scala.concurrent
import scala.concurrent.{ExecutionContext, Future}

@Singleton
class ProfilerInstances @Inject()(profilerInstanceRepo: ProfilerInstanceRepo, submitterManager:
                                  ProfilerSubmitterManager, profilerConfigValidator: ProfilerConfigValidator,
                                  yarnInteractor: YarnInteractor,
                                  assetSelectorDefinitionRepo: AssetSelectorDefinitionRepo)
                                 (implicit exec: ExecutionContext)
                                  extends Controller with JsonApi {

  import domain.JsonFormatters._
  val logger = Logger(classOf[ProfilerInstances])

  def indexAction = Action {
    Ok(index.render())
  }

  def all = Action.async {
    profilerInstanceRepo.findAll().map(profilers => Ok(Json.toJson(profilers)))
      .recover(mapError)
  }

  def getByName(name: String) = Action.async {
    profilerInstanceRepo.findByNameOpt(name).map {
      profilerOpt =>
        profilerOpt.map(profiler => Ok(Json.toJson(profiler))).getOrElse(NotFound)
    }.recover(mapError)
  }

  def insert = Action.async(parse.json) { req =>
    req.body
      .validate[ProfilerInstance] match {
      case JsSuccess(profiler, path) => {
        profilerConfigValidator.validateProfilerConfigs(profiler).flatMap(validatedProfilerInstanceConfig => {
            profilerInstanceRepo.save(validatedProfilerInstanceConfig.copy(version = 1, created = Some(DateTime.now()))).map {
              p =>
                if (p.profilerInstance.active) {
                  submitterManager.addProfilerInstance(p)
                }
                Ok(Json.toJson(p))
            }}).recover(mapError)
          }
      case e: JsError =>
        Future.successful(BadRequest(JsError.toFlatForm(e).toString()))
    }
  }

  def updateState(name: String, active: Boolean) = Action.async { req =>
    profilerInstanceRepo.findByNameOpt(name).flatMap {
      case Some(profiler) =>
        if (profiler.profilerInstance.active != active) {
          profilerInstanceRepo.updateState(name, active)
            .map { e =>
              if (active) {
                logger.info(s"Marking profiler : ${name} as active")
                submitterManager.addProfilerInstance(e)
              } else {
                logger.info(s"Marking profiler : ${name} as inactive")
                submitterManager.stopProfilerInstance(e.profilerInstance.name)
              }
              Ok(Json.obj(
                "name" -> name,
                "state" -> active,
                "changed" -> true
              ))
            }
        } else Future.successful(Ok(Json.obj(
          "name" -> name,
          "state" -> active,
          "changed" -> false
        )))
      case None => Future.successful(NotFound(Json.toJson(Error("404", s"Profiler instance ${name} does not exist"))))
    }
  }


  def update = Action.async(parse.json) { req =>
    req.body
      .validate[ProfilerInstance] match {
      case JsSuccess(profiler, path) => {
            profilerConfigValidator.validateProfilerConfigs(profiler).flatMap(validatedProfilerInstanceConfig => {
              profilerInstanceRepo.update(validatedProfilerInstanceConfig).map {
                p =>
                  if (p.profilerInstance.active) {
                    submitterManager.updateProfilerInstance(p)
                  }
                  logger.info(s"Profiler instance updated. New profiler instance => ${Json.toJson(profiler).toString}")
                  Ok(Json.toJson(p))
            }}).recover(mapError)
      }
      case e: JsError =>
        Future.successful(BadRequest(JsError.toFlatForm(e).toString()))
    }
  }

  def updateProfilerInstanceAndSelector(overwrite:Option[Boolean]) = Action.async(parse.json) { req =>
    req.body
      .validate[ProfilerInstanceAndSelectorConfig] match {
      case JsSuccess(profilerInstanceAndSelectorConfig, path) => {
        profilerConfigValidator.validateProfilerConfigs(profilerInstanceAndSelectorConfig.profilerInstance).flatMap(
          validatedProfilerInstanceConfig => {
            profilerConfigValidator.validateSelectorCron(profilerInstanceAndSelectorConfig.selectorConfig).flatMap(
              selectorCronSuccess => {
                val selectorConfig = profilerInstanceAndSelectorConfig.selectorConfig
                assetSelectorDefinitionRepo.findByName(selectorConfig.name).flatMap {
                  case Some(sel) =>
                    val newSelectorConfig = if(overwrite.getOrElse(false)) {
                      selectorConfig
                    } else selectorConfig.copy(config = sel.config ++ selectorConfig.config)

                    profilerInstanceRepo.updateProfilerInstanceAndSelectorTransaction(validatedProfilerInstanceConfig,
                      newSelectorConfig).map {
                      p =>
                        if (p.profilerInstance.active) {
                          submitterManager.updateProfilerInstance(p)
                          submitterManager.removeSelectorByName(selectorConfig.name)
                          val newAssetSelectorDefinition = sel.copy(config = newSelectorConfig.config)
                          submitterManager.addSelectorDefinition(newAssetSelectorDefinition)
                        }
                        logger.info(s"Profiler instance selector config updated. New value => ${Json.toJson(profilerInstanceAndSelectorConfig).toString}")
                        Ok(Json.toJson(p))
                    }
                  case None =>
                    Future.failed(new InstanceNotFoundException("404", s"Selector ${selectorConfig.name} does not exist"))
        }})}).recover(mapError)
      }
      case e: JsError =>
        Future.successful(BadRequest(JsError.toFlatForm(e).toString()))
    }
  }

  private def getValidatedProfilerInstanceAndSelector(profilerInstanceAndSelectorConfig:
                                         ProfilerInstanceAndSelectorConfig, overwrite:Option[Boolean])
                                          :Future[ProfilerInstanceAndSelectorDefinition] = {
    profilerConfigValidator.validateProfilerConfigs(profilerInstanceAndSelectorConfig.profilerInstance).flatMap {
      validatedProfilerInstanceConfig => {
        profilerConfigValidator.validateSelectorCron(profilerInstanceAndSelectorConfig.selectorConfig).flatMap(
          selectorCronSuccess => {
            assetSelectorDefinitionRepo.findByName(profilerInstanceAndSelectorConfig.selectorConfig.name).flatMap {
              case Some(selector) => {
                val updatedSelector = if (overwrite.getOrElse(false)) {
                  selector.copy(config = profilerInstanceAndSelectorConfig.selectorConfig.config)
                } else selector.copy(config =
                  selector.config ++ profilerInstanceAndSelectorConfig.selectorConfig.config)
                Future.successful(new ProfilerInstanceAndSelectorDefinition(validatedProfilerInstanceConfig, updatedSelector))
              }
              case None => {
                Future.failed(new InstanceNotFoundException("404",
                  s"Selector ${profilerInstanceAndSelectorConfig.selectorConfig.name} does not exist"))
              }
            }
          }
        )
      }
    }
  }

  def updateProfilerInstanceAndSelectorBulk(overwrite: Option[Boolean]) = Action.async(parse.json) { req =>
    req.body
      .validate[List[ProfilerInstanceAndSelectorConfig]] match {
      case JsSuccess(list, path) => {
        val listOfValidatedFuture = Future.sequence(list.map(getValidatedProfilerInstanceAndSelector(_, overwrite)))

        listOfValidatedFuture.flatMap(pInstAndSelList => {
          val profilerNameToSelectorMap = pInstAndSelList.map(obj => obj.profilerInstance.name -> obj.selectorDefinition).toMap
          profilerInstanceRepo.updateProfilerInstanceAndSelectorListTransaction(pInstAndSelList)
            .flatMap(updatedList => {updatedList.foreach {obj =>
            {
              if (obj.profilerInstance.active) {
                val updatedSelector = profilerNameToSelectorMap(obj.profilerInstance.name)
                submitterManager.updateProfilerInstance(_)
                submitterManager.removeSelectorByName(updatedSelector.name)
                submitterManager.addSelectorDefinition(updatedSelector)
              }
            }
            }
              Future.successful(Ok(Json.toJson(pInstAndSelList)))
            })
        }).recover(mapError)
      }
      case e: JsError =>
        Future.successful(BadRequest(JsError.toFlatForm(e).toString()))
    }
  }
}
