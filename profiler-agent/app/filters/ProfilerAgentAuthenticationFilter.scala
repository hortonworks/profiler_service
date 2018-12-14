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
package filters

import akka.stream.Materializer
import javax.inject.Inject
import play.api.{Configuration, Logger}
import play.api.mvc.{Filter, RequestHeader, Result}

import scala.concurrent.{ExecutionContext, Future}

class ProfilerAgentAuthenticationFilter @Inject()(implicit val mat: Materializer, ec: ExecutionContext, config: Configuration)
  extends Filter{
  private val isKerberosEnabled = config.getBoolean("dpprofiler.secured").getOrElse(false)
  private val spnegoFilterOption = getSpnegoFilterOption()
  private val knoxSSOEnabled = config.getBoolean("dpprofiler.sso.knox.enabled").getOrElse(false)
  private val knoxSSOFilterOption = getKnoxSSOAuthenticatorOption()
  private val ssoCookieName = config.getString("dpprofiler.sso.knox.cookiename").getOrElse("hadoop-jwt")

  private def getKnoxSSOAuthenticatorOption():Option[ProfilerAgentKnoxSSOFilter] = {
    knoxSSOEnabled match {
      case true => Some(new ProfilerAgentKnoxSSOFilter())
      case false => None
    }
  }

  private def getSpnegoFilterOption():Option[SpnegoAuthenticationFilter] = {
    isKerberosEnabled match {
      case true => Some(new SpnegoAuthenticationFilter())
      case false => None
    }
  }

  def apply(nextFilter: RequestHeader => Future[Result])
           (requestHeader: RequestHeader): Future[Result] = {

    val performSSOIfPossible = knoxSSOFilterOption.flatMap(
      ssoFilter => {
        requestHeader.cookies.get(ssoCookieName).map(
          _ => ssoFilter.performSSOAuthentication(nextFilter, requestHeader)
        )
      }
    )

    performSSOIfPossible.getOrElse(
      spnegoFilterOption.map(
        spnegoFilter =>
          spnegoFilter.performSpnegoAuthentication(nextFilter, requestHeader, spnegoFilter.spnegoAuthenticator)
      ).getOrElse(nextFilter(requestHeader))
    )
  }
}
