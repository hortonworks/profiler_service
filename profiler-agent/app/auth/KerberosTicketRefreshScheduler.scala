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
package auth

import java.util
import javax.inject.{Inject, Singleton}
import org.quartz.impl.StdSchedulerFactory
import org.quartz._
import play.api.{Configuration, Logger}
import job.scheduler.Scheduler
import auth.KerberosTicketRefresh


@Singleton
class KerberosTicketRefreshScheduler @Inject()(configuration: Configuration) {
  private val scheduler = Scheduler.getScheduler
  private val kerberosTicketRefreshCron = configuration.getString("dpprofiler.kerberos.ticket.refresh.cron").getOrElse("00 */6 * * *")

  startRefreshSchedule()

  private  def startRefreshSchedule() = {
    val isSecured = configuration.getBoolean("dpprofiler.secured").getOrElse(false)
    if(isSecured){
      val principal = configuration.getString("dpprofiler.kerberos.principal").getOrElse("")
      val keytab = configuration.getString("dpprofiler.kerberos.keytab").getOrElse("")
      val retryAllowed = configuration.getInt("dpprofiler.kerberos.ticket.refresh.retry.allowed").getOrElse(5)
      val ticketCredentials = TicketCredentials(principal, keytab, 0, retryAllowed)
      Logger.info("Refreshing dpprofiler Kerberos ticket on Profiler Agent server start")
      KerberosTicketRefresh.refreshTicket(ticketCredentials)

      Logger.info("Scheduling new cron job to refresh dpprofiler Kerberos ticket periodically")
      val trigger = TriggerBuilder.newTrigger()
        .withIdentity(s"trigger-ticket-refresh", s"group-ticket-refresh")
        .withSchedule(CronScheduleBuilder.cronSchedule(kerberosTicketRefreshCron))
        .build()

      val map = new util.HashMap[String, Object]()
      map.put("ticketCredentials", ticketCredentials)

      val jobName = s"job-ticket-refresh"
      val groupName = s"group-ticket-refresh"
      val job = JobBuilder.newJob(classOf[KerberosTicketRefreshJob])
        .withIdentity(jobName, groupName)
        .usingJobData(new JobDataMap(map))
        .build()

      if (!scheduler.checkExists(new JobKey(jobName, groupName))) {
        scheduler.scheduleJob(job, trigger)
      }
    }
  }
}
