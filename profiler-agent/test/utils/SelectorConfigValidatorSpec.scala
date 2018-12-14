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
import domain.{ProfilerConfigValidationException, SelectorConfig}

import scala.concurrent.Future

class SelectorConfigValidatorSpec extends PlaySpec with FutureAwaits with DefaultAwaitTimeout with MockitoSugar{
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

  val selectorConfigObj = SelectorConfig(name = "test_selector",
    config = Json.obj("cronExpr" -> "0 0 0/1 1/1 * ? *"))

  "Selector Config validator " must {
    " return success on a valid CRON expression in request " in {
      val result = profilerConfigValidator.validateSelectorCron(selectorConfigObj)
      await(result)
      val resultAsBoolean = result.value.get.get
      assert(resultAsBoolean)
    }

    " throw validation exception with an invalid CRON expression in request " in {
      val invalidSelectorConfig = selectorConfigObj.copy(config = Json.obj("cronExpr" -> "some random string"))
      val caughtException = intercept[ProfilerConfigValidationException]{
        val result = profilerConfigValidator.validateSelectorCron(invalidSelectorConfig)
        await(result)
      }
      assert(caughtException.code == "400")
      assert(caughtException.message == "Cron expression some random string is not valid")
    }

    " throw validation exception when the schedule is too frequent " in {
      val tooFrequentSelectorConfig = selectorConfigObj.copy(config = Json.obj("cronExpr" -> "* * * * * ?"))
      val caughtException = intercept[ProfilerConfigValidationException]{
        val result = profilerConfigValidator.validateSelectorCron(tooFrequentSelectorConfig)
        await(result)
      }
      assert(caughtException.code == "400")
      assert(caughtException.message == "Cron Expression * * * * * ? is too frequent")
    }

    " return success with too frequent CRON when check is disabled " in {
      when(configMock.getBoolean("scheduler.config.cron.frequency.check.enabled")).thenReturn(Some(false))
      val profilerConfigValidator = new ProfilerConfigValidator(yarnInteractorMock, configMock)
      val tooFrequentSelectorConfig = selectorConfigObj.copy(config = Json.obj("cronExpr" -> "* * * * * ?"))
      val result = profilerConfigValidator.validateSelectorCron(tooFrequentSelectorConfig)
      await(result)
      val resultAsBoolean = result.value.get.get
      assert(resultAsBoolean)
    }
  }
}
