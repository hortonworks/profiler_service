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

import org.scalatest.mockito.MockitoSugar
import org.scalatestplus.play.PlaySpec
import play.api.test.{DefaultAwaitTimeout, FutureAwaits}
import play.api.Configuration
import org.mockito.Mockito._
import play.api.libs.json.Json
import domain.{ProfilerConfigValidationException, ProfilerInstance}
import org.joda.time.DateTime

import scala.concurrent.Future

class ProfilerConfigValidatorSpec extends PlaySpec with FutureAwaits with DefaultAwaitTimeout with MockitoSugar{
  val yarnInteractorMock =  mock[YarnInteractor]
  val configMock = mock[Configuration]
  when(yarnInteractorMock.getYarnQueueSet()).thenReturn(Future.successful(Set("default", "testqueue")))
  when(configMock.getInt("profiler.job.executor.memory.min")).thenReturn(Some(1))
  when(configMock.getInt("profiler.job.executor.memory.max")).thenReturn(Some(10))
  when(configMock.getInt("profiler.job.executor.cores.min")).thenReturn(Some(1))
  when(configMock.getInt("profiler.job.executor.cores.max")).thenReturn(Some(10))
  when(configMock.getInt("profiler.job.executor.count.min")).thenReturn(Some(1))
  when(configMock.getInt("profiler.job.executor.count.max")).thenReturn(Some(50))
  when(configMock.getInt("profiler.job.driver.cores.min")).thenReturn(Some(1))
  when(configMock.getInt("profiler.job.driver.cores.max")).thenReturn(Some(10))
  when(configMock.getInt("profiler.job.driver.memory.min")).thenReturn(Some(1))
  when(configMock.getInt("profiler.job.driver.memory.max")).thenReturn(Some(10))
  when(configMock.getBoolean("scheduler.config.cron.frequency.check.enabled")).thenReturn(Some(true))

  import scala.concurrent.ExecutionContext.Implicits.global
  val profilerConfigValidator = new ProfilerConfigValidator(yarnInteractorMock, configMock)
  val profilerSparkConf = Json.obj("numExecutors" -> 2,
    "executorCores" -> 1,
    "executorMemory" -> "1g",
    "driverCores" -> 1,
    "driverMemory" -> "1g")
  val profilerInstanceObj = ProfilerInstance(id = None, name = "test",
    displayName = "test profiler instance", profilerId = 1L,
    profilerConf = profilerSparkConf, jobConf = Json.obj(), queue = "testqueue", description = "test profiler instance",
       created = Some(DateTime.now()), modified = None)

  //Positive tests
  "Profiler validator " must {
    " return success when the attributes are in expected ranges " in {
      val result = profilerConfigValidator.validateProfilerConfigs(profilerInstanceObj)
      await(result)
      val resultAsProfilerInstance = result.value.get.get
      assert(resultAsProfilerInstance.isInstanceOf[ProfilerInstance])
      assert(resultAsProfilerInstance.queue.equals("testqueue"))
      val updatedProfilerConf = resultAsProfilerInstance.profilerConf
      assert((updatedProfilerConf \ "numExecutors").as[Int] == 2)
      assert((updatedProfilerConf \ "executorCores").as[Int] == 1)
      assert((updatedProfilerConf \ "executorMemory").as[String] == "1g")
      assert((updatedProfilerConf \ "driverCores").as[Int] == 1)
      assert((updatedProfilerConf \ "driverMemory").as[String] == "1g")
    }

    " not fail and fill with default when empty value is passed in numExecutor field " in {
        val profilerSparkConfigWithEmptyValue = profilerSparkConf ++ Json.obj("numExecutors" -> "")
        val profilerInstanceObjWithEmptyValue = profilerInstanceObj.copy(profilerConf = profilerSparkConfigWithEmptyValue)
        val result = profilerConfigValidator.validateProfilerConfigs(profilerInstanceObjWithEmptyValue)
        await(result)
        val resultAsProfilerInstance = result.value.get.get
        assert(resultAsProfilerInstance.isInstanceOf[ProfilerInstance])
        val updatedProfilerConf = resultAsProfilerInstance.profilerConf
        assert((updatedProfilerConf \ "numExecutors").as[Int] == 1)
    }

    " not fail when integer value as string is passed in numExecutor field " in {
      val profilerSparkConfigWithEmptyValue = profilerSparkConf ++ Json.obj("numExecutors" -> "2")
      val profilerInstanceObjWithEmptyValue = profilerInstanceObj.copy(profilerConf = profilerSparkConfigWithEmptyValue)
      val result = profilerConfigValidator.validateProfilerConfigs(profilerInstanceObjWithEmptyValue)
      await(result)
      val resultAsProfilerInstance = result.value.get.get
      assert(resultAsProfilerInstance.isInstanceOf[ProfilerInstance])
      val updatedProfilerConf = resultAsProfilerInstance.profilerConf
      assert((updatedProfilerConf \ "numExecutors").as[Int] == 2)
    }

    "not fail and fill with default when empty value is passed in executorCores field" in {
      val profilerSparkConfigWithEmptyValue = profilerSparkConf ++ Json.obj("executorCores" -> "")
      val profilerInstanceObjWithEmptyValue = profilerInstanceObj.copy(profilerConf = profilerSparkConfigWithEmptyValue)
      val result = profilerConfigValidator.validateProfilerConfigs(profilerInstanceObjWithEmptyValue)
      await(result)
      val resultAsProfilerInstance = result.value.get.get
      assert(resultAsProfilerInstance.isInstanceOf[ProfilerInstance])
      val updatedProfilerConf = resultAsProfilerInstance.profilerConf
      assert((updatedProfilerConf \ "executorCores").as[Int] == 1)
    }

    "not fail and fill with default when empty value is passed in driverCores field" in {
      val profilerSparkConfigWithEmptyValue = profilerSparkConf ++ Json.obj("driverCores" -> "")
      val profilerInstanceObjWithEmptyValue = profilerInstanceObj.copy(profilerConf = profilerSparkConfigWithEmptyValue)
      val result = profilerConfigValidator.validateProfilerConfigs(profilerInstanceObjWithEmptyValue)
      await(result)
      val resultAsProfilerInstance = result.value.get.get
      assert(resultAsProfilerInstance.isInstanceOf[ProfilerInstance])
      val updatedProfilerConf = resultAsProfilerInstance.profilerConf
      assert((updatedProfilerConf \ "driverCores").as[Int] == 1)
    }

    "not fail and fill with default when empty value is passed in executorMemory field" in {
      val profilerSparkConfigWithEmptyValue = profilerSparkConf ++ Json.obj("executorMemory" -> "")
      val profilerInstanceObjWithEmptyValue = profilerInstanceObj.copy(profilerConf = profilerSparkConfigWithEmptyValue)
      val result = profilerConfigValidator.validateProfilerConfigs(profilerInstanceObjWithEmptyValue)
      await(result)
      val resultAsProfilerInstance = result.value.get.get
      assert(resultAsProfilerInstance.isInstanceOf[ProfilerInstance])
      val updatedProfilerConf = resultAsProfilerInstance.profilerConf
      assert((updatedProfilerConf \ "executorMemory").as[String] == "1g")
    }

    "not fail and fill with default when empty value is passed in driverMemory field" in {
      val profilerSparkConfigWithEmptyValue = profilerSparkConf ++ Json.obj("driverMemory" -> "")
      val profilerInstanceObjWithEmptyValue = profilerInstanceObj.copy(profilerConf = profilerSparkConfigWithEmptyValue)
      val result = profilerConfigValidator.validateProfilerConfigs(profilerInstanceObjWithEmptyValue)
      await(result)
      val resultAsProfilerInstance = result.value.get.get
      assert(resultAsProfilerInstance.isInstanceOf[ProfilerInstance])
      val updatedProfilerConf = resultAsProfilerInstance.profilerConf
      assert((updatedProfilerConf \ "driverMemory").as[String] == "1g")
    }
  }

  //Negative tests
  "Profiler validator " must {
    " throw exception when invalid queue is specified in new config" in {
      val profilerInstanceObjWithInvalidQueue = profilerInstanceObj.copy(queue = "randomqueue")
      val caughtException = intercept[ProfilerConfigValidationException] {
        val result = profilerConfigValidator.validateProfilerConfigs(profilerInstanceObjWithInvalidQueue)
        await(result)
      }
      assert(caughtException.code == "400")
      assert(caughtException.message == "Invalid queue randomqueue specified in profiler configurations")
    }

    " fail when negative value is passed in numExecutors field " in {
      val profilerSparkConfigWithEmptyValue = profilerSparkConf ++ Json.obj("numExecutors" -> "-1")
      val profilerInstanceObjWithEmptyValue = profilerInstanceObj.copy(profilerConf = profilerSparkConfigWithEmptyValue)
      val caughtException = intercept[ProfilerConfigValidationException]{
        val result = profilerConfigValidator.validateProfilerConfigs(profilerInstanceObjWithEmptyValue)
        await(result)
      }
      assert(caughtException.code == "400")
      assert(caughtException.message == "numExecutors value -1 is not valid")
    }

    " fail when value higher than maximum allowed value is passed in numExecutors field " in {
      val profilerSparkConfigWithEmptyValue = profilerSparkConf ++ Json.obj("numExecutors" -> "1000")
      val profilerInstanceObjWithEmptyValue = profilerInstanceObj.copy(profilerConf = profilerSparkConfigWithEmptyValue)
      val caughtException = intercept[ProfilerConfigValidationException]{
        val result = profilerConfigValidator.validateProfilerConfigs(profilerInstanceObjWithEmptyValue)
        await(result)
      }
      assert(caughtException.code == "400")
      assert(caughtException.message == "numExecutors value 1000 is not valid")
    }
  }
}
