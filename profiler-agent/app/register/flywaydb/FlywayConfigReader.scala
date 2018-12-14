/*
 * HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
 * (c) 2016-2018 Hortonworks, Inc. All rights reserved.
 * This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
 * Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
 * to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
 * properly licensed third party, you do not have any rights to this code.
 * If this code is provided to you under the terms of the AGPLv3:
 * (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
 * (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
 *   LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
 * (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
 *   FROM OR RELATED TO THE CODE; AND
 * (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
 *   DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
 *   DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
 *   OR LOSS OR CORRUPTION OF DATA.
 */

package register.flywaydb

import javax.inject.{Inject, Singleton}

import play.api.Configuration


@Singleton
class FlywayConfigReader @Inject()(configuration: Configuration) {

  def getFlywayConfiguration(dbName: String) = {

    val database = getDatabaseConfiguration(configuration, dbName)
    val subConfig: Configuration = configuration.getConfig(s"db.$dbName.migration").getOrElse(Configuration.empty)

    val placeholders = {
      subConfig.getConfig("placeholders").map { config =>
        config.subKeys.map { key => key -> config.getString(key).getOrElse("") }.toMap
      }.getOrElse(Map.empty)
    }

    FlywayConfiguration(
      database,
      subConfig.getStringSeq("locations").getOrElse(Seq.empty[String]),
      subConfig.getString("encoding"),
      subConfig.getStringSeq("schemas").getOrElse(Seq.empty[String]),
      subConfig.getString("table"),
      subConfig.getBoolean("placeholderReplacement"),
      placeholders,
      subConfig.getString("placeholderPrefix"),
      subConfig.getString("placeholderSuffix"),
      subConfig.getString("sqlMigrationPrefix"),
      subConfig.getString("repeatableSqlMigrationPrefix"),
      subConfig.getString("sqlMigrationSeparator"),
      subConfig.getStringSeq("sqlMigrationSuffixes").getOrElse(Seq.empty[String]),
      subConfig.getBoolean("ignoreFutureMigrations"),
      subConfig.getBoolean("validateOnMigrate"),
      subConfig.getBoolean("cleanOnValidationError"),
      subConfig.getBoolean("cleanDisabled"),
      subConfig.getBoolean("initOnMigrate"),
      subConfig.getBoolean("outOfOrder"))
  }

  private def getDatabaseConfiguration(configuration: Configuration, dbName: String): DatabaseConfiguration = {
    val driver = configuration.getString(s"db.$dbName.driver").get
    val jdbcUrl = configuration.getString(s"db.$dbName.url").get
    val username = configuration.getString(s"db.$dbName.username").get
    val password = configuration.getString(s"db.$dbName.password").get

    DatabaseConfiguration(driver, jdbcUrl, username, password)
  }
}
