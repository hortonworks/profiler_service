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

package job.runner

import domain.JobType
import domain.JobType.JobType
import javax.inject.Inject
import play.api.libs.ws.WSClient
import play.api.{Configuration, Logger}

class JobRunners @Inject()(configuration: Configuration, ws: WSClient, jobDataBuilder: JobDataBuilder) {

  private val secured = configuration.getBoolean("dpprofiler.secured").getOrElse(false)

  Logger.info(s"Spenego env properties : $secured")
  Logger.info(s"java.security.krb5.conf : ${System.getProperty("java.security.krb5.conf")}")
  Logger.info(s"javax.security.auth.useSubjectCredsOnly : ${System.getProperty("javax.security.auth.useSubjectCredsOnly")}")
  Logger.info(s"java.security.auth.login.config : ${System.getProperty("java.security.auth.login.config")}")

  private val extraJars: List[String] = {
    val extraJarsPropertyValue: String = configuration.getString("dpprofiler.extra.jars").getOrElse("").trim

    if (extraJarsPropertyValue.isEmpty) {
      Nil
    }
    else {
      extraJarsPropertyValue.split(",").toList
    }
  }


  private val livyRunnerOpt = getLivyJobRunner()


  private val livyRunnerMap: Map[JobType, JobRunner] = livyRunnerOpt match {
    case Some(runner) => Map(JobType.Livy -> runner)
    case None => Map()
  }

  private val jobRunnerMap = livyRunnerMap

  def runners: Map[JobType, JobRunner] = jobRunnerMap

  private def getLivyJobRunner(): Option[LivyJobRunner] = {
    configuration.getConfig("livy") flatMap {
      config =>
        val livyConf: Map[String, String] = getConfigAsMap(config)
        if (!livyConf.contains("url")) {
          Logger.warn("Livy config does not contain url. Wont able to run Livy Jobs")
          None
        } else {
          Some(new LivyJobRunner(extraJars, livyConf, ws, jobDataBuilder, secured))
        }
    }
  }

  private def getConfigAsMap(config: Configuration) = {
    import collection.JavaConverters._
    config.underlying.entrySet().asScala.map(e => (e.getKey, e.getValue.unwrapped().toString)).toMap
  }
}
