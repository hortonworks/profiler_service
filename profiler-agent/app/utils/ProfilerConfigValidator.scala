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
package utils

import javax.inject.{Inject, Singleton}
import play.api.mvc.Controller
import play.api.{Configuration, Logger}
import domain.{ProfilerConfigValidationException, ProfilerInstance, SelectorConfig}
import org.quartz._
import play.api.libs.json.{JsValue, Json}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

@Singleton
class ProfilerConfigValidator @Inject()(yarnInteractor: YarnInteractor, config: Configuration)(implicit exec: ExecutionContext) extends Controller{

  private val attributeToRangeMap = Map("executorMemory" -> List(config.getInt("profiler.job.executor.memory.min").getOrElse(1),
    config.getInt("profiler.job.executor.memory.max").getOrElse(10)),
  "driverMemory" -> List(config.getInt("profiler.job.driver.memory.min").getOrElse(1),
    config.getInt("profiler.job.driver.memory.max").getOrElse(10)),
  "numExecutors" -> List(config.getInt("profiler.job.executor.count.min").getOrElse(1),
    config.getInt("profiler.job.executor.count.max").getOrElse(50)),
  "executorCores" -> List(config.getInt("profiler.job.executor.cores.min").getOrElse(1),
    config.getInt("profiler.job.executor.cores.max").getOrElse(10)),
  "driverCores" -> List(config.getInt("profiler.job.driver.cores.min").getOrElse(1),
    config.getInt("profiler.job.driver.cores.max").getOrElse(10)))

  private val cronFrequencyCheckEnabled = config.getBoolean("scheduler.config.cron.frequency.check.enabled").getOrElse(true)
  val logger = Logger(classOf[ProfilerConfigValidator])

  private def validateYarnQueue(profilerInstance: ProfilerInstance): Future[Int] = {
    val yarnQueueSet = yarnInteractor.getYarnQueueSet()
    yarnQueueSet.flatMap(queues =>
      if(queues.contains(profilerInstance.queue)){
        logger.info("Queue validated successfully")
        Future.successful(1)
      }else{
        logger.error(s"Invalid queue ${profilerInstance.queue} specified in profiler configuration update request")
        Future.failed(new ProfilerConfigValidationException("400", s"Invalid queue " +
          s"${profilerInstance.queue} specified in profiler configurations"))
      }
    )
  }

  private def validateStringAttributes(profilerInstance: ProfilerInstance, attributeName:String):Future[String] = {
    (profilerInstance.profilerConf \ attributeName).asOpt[String] match {
      case Some(attributeValue) => {
        val attributeValueAsIntTry = Try( attributeValue.stripSuffix("g").toInt )
        attributeValueAsIntTry match {
          case Success(attributeValueAsInt) => {
            if(attributeValueAsInt >= attributeToRangeMap(attributeName)(0) && attributeValueAsInt <= attributeToRangeMap(attributeName)(1)) {
              Future.successful(attributeValueAsInt.toString + "g")
            }else{
              logger.error(s"${attributeName} ${attributeValue} in profiler configuration update request is not valid")
              Future.failed(new ProfilerConfigValidationException("400", s"${attributeName} value ${attributeValue} is not valid"))
            }
          }
          case Failure(e) => {
            attributeValue.length match {
              case 0 => {
                logger.info(s"Found empty value for attribute ${attributeName}. Will use the default value of 1g")
                Future.successful("1g")
              }
              case _ => {
                logger.info(s"Encountered exception while casting value for attribute ${attributeName} to integer ${e.getMessage}")
                Future.failed(new ProfilerConfigValidationException("400", s"${attributeName} value ${attributeValue} is not valid"))
              }
            }
          }
        }
      }
      case None => {
        logger.info(s"No value specified for ${attributeName} in new profiler configuration. Will use the default value of 1g")
        Future.successful("1g")
      }
    }
  }

  private def validateIntegerAttributes(profilerInstance:ProfilerInstance, attributeName:String):Future[Int] = {
    (profilerInstance.profilerConf \ attributeName).asOpt[JsValue] match {
      case Some(attributeValue) => {
        val attributeValueAsIntTry = Try(attributeValue.toString.stripSuffix("\"").stripPrefix("\"").toInt)
        attributeValueAsIntTry match {
          case Success(attributeValueAsInt) => {
            if(attributeValueAsInt >= attributeToRangeMap(attributeName)(0) && attributeValueAsInt <= attributeToRangeMap(attributeName)(1)) {
              Future.successful(attributeValueAsInt)
            }else{
              logger.error(s"${attributeName} ${attributeValue} in profiler configuration update request is not valid")
              Future.failed(new ProfilerConfigValidationException("400", s"${attributeName} value ${attributeValue} is not valid"))
            }
          }
          case Failure(e) => {
            logger.info(s"Encountered exception while casting value for attribute ${attributeName} to integer ${e.getMessage}")
            Future.failed(new ProfilerConfigValidationException("400", s"${attributeName} value ${attributeValue} is not valid"))
          }
        }
      }
      case None => {
            logger.info(s"No value specified for ${attributeName} in new profiler configuration. Will use the default value")
            Future.successful(1)
        }
      }
  }

  private def validateSamplePercent(profilerInstance: ProfilerInstance):Future[Int] = {
    val jobConfigs = profilerInstance.jobConf
    jobConfigs.keys.contains("samplepercent") match {
      case true => {
        val samplePercent = (jobConfigs \ "samplepercent").as[String].toInt
        if(samplePercent >= 1 && samplePercent <= 100){
          Future.successful(1)
        }else{
          logger.error(s"Sample percent ${samplePercent.toString} is not valid")
          Future.failed(new ProfilerConfigValidationException("400", s"Sample percent ${samplePercent.toString} is not valid"))
        }
      }
      case false => Future.successful(1)
    }
  }

  private def validateSampleRows(profilerInstance: ProfilerInstance):Future[Int] = {
    val jobConfigs = profilerInstance.jobConf
    jobConfigs.keys.contains("sampleSize") match {
      case true => {
        val sampleSize = (jobConfigs \ "sampleSize").as[String].toInt
        if(sampleSize >= 100 && sampleSize <= 100000){
          Future.successful(1)
        }else{
          logger.error(s"Sample size ${sampleSize.toString} is not valid")
          Future.failed(new ProfilerConfigValidationException("400", s"Sample size ${sampleSize.toString} is not valid"))
        }
      }
      case false => Future.successful(1)
    }
  }

  def validateProfilerConfigs(profilerInstance: ProfilerInstance): Future[ProfilerInstance] = {
    validateYarnQueue(profilerInstance).flatMap(a =>
      validateStringAttributes(profilerInstance, "executorMemory").flatMap(validatedExecutorMemory =>
        validateIntegerAttributes(profilerInstance, "executorCores").flatMap(validatedExecutorCores =>
          validateIntegerAttributes(profilerInstance, "numExecutors").flatMap(validatedExecutorCount =>
            validateIntegerAttributes(profilerInstance, "driverCores").flatMap(validatedDriverCores =>
              validateStringAttributes(profilerInstance, "driverMemory").flatMap(validatedDriverMemory =>
                validateSamplePercent(profilerInstance).flatMap(g =>
                  validateSampleRows(profilerInstance).flatMap(h => {
                    val validatedAndFormattedConfigs = profilerInstance.profilerConf ++
                      Json.obj("executorMemory" -> validatedExecutorMemory,
                        "executorCores" -> validatedExecutorCores,
                        "numExecutors" -> validatedExecutorCount,
                        "driverCores" -> validatedDriverCores,
                        "driverMemory" -> validatedDriverMemory
                      )
                    Future.successful(profilerInstance.copy(profilerConf = validatedAndFormattedConfigs))
                  })))))))
    )
  }

  def validateCronExpr(cronExpr:String):Future[Int] = {
    if(CronExpression.isValidExpression(cronExpr)){
      Future.successful(1)
    }else{
      logger.error(s"Cron expression ${cronExpr} in profiler configuration update request is not valid")
      Future.failed(new ProfilerConfigValidationException("400", s"Cron expression ${cronExpr} is not valid"))
    }
  }

  def isScheduleTooFrequent(frequency:String):Boolean = {
    frequency.equals("*") || frequency.contains("/")
  }

  def validateCronExprFrequency(cronExpr:String):Future[Int] = {
    val cronParts = cronExpr.split(" ")
    val secondFrequency = cronParts(0)
    val minuteFrequency = cronParts(1)
    (isScheduleTooFrequent(secondFrequency) || isScheduleTooFrequent(minuteFrequency)) match {
      case true => Future.failed(new ProfilerConfigValidationException("400", s"Cron Expression ${cronExpr} is too frequent"))
      case false => Future.successful(1)
    }
  }

  def validateSelectorCron(selectorConfig: SelectorConfig): Future[Boolean] = {
    val cronExpression = ( selectorConfig.config \ "cronExpr").as[String]
    validateCronExpr(cronExpression).flatMap(a =>
      {
        if(cronFrequencyCheckEnabled){
          validateCronExprFrequency(cronExpression).flatMap(b =>
            Future.successful(true))
        }else{
          Logger.info(s"Cron frequency check is not enabled. Skipping the cron frequency validation")
          Future.successful(true)
        }
      })
  }
}
