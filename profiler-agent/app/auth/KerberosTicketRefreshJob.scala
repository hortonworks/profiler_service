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

import java.util.concurrent.Executors

import org.quartz.{Job, JobExecutionContext}
import play.api.Logger

import scala.concurrent.{ExecutionContext, Future}
import sys.process._

class KerberosTicketRefreshJob extends Job{
  override def execute(context: JobExecutionContext) = {
    val ticketCredentials = context.getMergedJobDataMap().get("ticketCredentials").asInstanceOf[TicketCredentials]
    KerberosTicketRefresh.refreshTicket(ticketCredentials)
  }
}

object KerberosTicketRefresh {
  implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(2))
  def refreshTicket(ticketCredentials: TicketCredentials):Unit = {
      val kinitCommand = s"kinit -kt ${ticketCredentials.keytab} ${ticketCredentials.principal}"
      Logger.info(s"Running Kerberos ticket refresh command ${kinitCommand}")
      val exitCode = {
        kinitCommand !
      }
      if (exitCode == 0){
        Logger.info("Kerberos ticket refresh for dpprofiler successful")
      }else{
        if (ticketCredentials.retryCount < ticketCredentials.retryAllowed){
          Logger.error("Kerberos ticket refresh for dpprofiler failed. Retrying")
          KerberosTicketRefresh.refreshTicket(ticketCredentials.copy(retryCount = ticketCredentials.retryCount + 1))
        }else{
          Logger.error("Kerberos ticket refresh for dpprofiler failed")
        }
      }
  }
}

case class TicketCredentials(principal: String, keytab: String, retryCount: Int, retryAllowed:Int)
