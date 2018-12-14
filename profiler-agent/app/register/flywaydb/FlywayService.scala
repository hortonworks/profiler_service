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

import com.hortonworks.dataplane.profilers.commons.security.ProfilerCredentialProvider
import org.flywaydb.core.Flyway
import org.flywaydb.core.internal.util.jdbc.DriverDataSource
import play.api.Configuration


@Singleton
class FlywayService @Inject()(config: Configuration, flywayConfigReader: FlywayConfigReader) {

  private val flywayPrefixToMigrationScript = "db/migration"

  private val passwordEncrytionEnabled = config.getBoolean("credential.store.enabled").getOrElse(false)
  private val doMigration = config.getBoolean("db.migration.onstart").getOrElse(true)

  private val credentialFile = config.getString("credential.provider.path").getOrElse("jceks://file/etc/profiler_agent/conf/dpprofiler-config.jceks")

  if (doMigration) {
    val flyway = getFlyway("default")
    flyway.migrate()
  }

  private def setSqlMigrationSuffixes(configuration: FlywayConfiguration, flyway: Flyway): Unit = {
    val suffixes: Seq[String] = configuration.sqlMigrationSuffixes
    if (suffixes.nonEmpty) flyway.setSqlMigrationSuffixes(suffixes: _*)
  }

  private def getDecryptedConfig(flywayConfiguration: FlywayConfiguration) = {
    if (passwordEncrytionEnabled) {
      val credentialProvider = new ProfilerCredentialProvider(credentialFile)
      val decrytedPassword = credentialProvider.resolveAlias(flywayConfiguration.database.password)
      flywayConfiguration.copy(database = flywayConfiguration.database.copy(password = decrytedPassword))
    } else flywayConfiguration
  }

  private def getFlyway(dbName: String) = {
    val flyway = new Flyway()


    val configuration = getDecryptedConfig(flywayConfigReader.getFlywayConfiguration(dbName))
    val migrationFilesLocation = s"$flywayPrefixToMigrationScript/$dbName"

    val database = configuration.database
    val dataSource = new DriverDataSource(
      getClass.getClassLoader,
      database.driver,
      database.url,
      database.user,
      database.password,
      null)
    flyway.setDataSource(dataSource)
    if (configuration.locations.nonEmpty) {
      val locations = configuration.locations.map(location => s"$migrationFilesLocation/$location")
      flyway.setLocations(locations: _*)
    } else {
      flyway.setLocations(migrationFilesLocation)
    }
    configuration.encoding.foreach(flyway.setEncoding)
    flyway.setSchemas(configuration.schemas: _*)
    configuration.table.foreach(flyway.setTable)
    configuration.placeholderReplacement.foreach(flyway.setPlaceholderReplacement)

    import scala.collection.JavaConverters._
    flyway.setPlaceholders(configuration.placeholders.asJava)

    configuration.placeholderPrefix.foreach(flyway.setPlaceholderPrefix)
    configuration.placeholderSuffix.foreach(flyway.setPlaceholderSuffix)
    configuration.sqlMigrationPrefix.foreach(flyway.setSqlMigrationPrefix)
    configuration.repeatableSqlMigrationPrefix.foreach(flyway.setRepeatableSqlMigrationPrefix)
    configuration.sqlMigrationSeparator.foreach(flyway.setSqlMigrationSeparator)
    setSqlMigrationSuffixes(configuration, flyway)
    configuration.ignoreFutureMigrations.foreach(flyway.setIgnoreFutureMigrations)
    configuration.validateOnMigrate.foreach(flyway.setValidateOnMigrate)
    configuration.cleanOnValidationError.foreach(flyway.setCleanOnValidationError)
    configuration.cleanDisabled.foreach(flyway.setCleanDisabled)
    configuration.initOnMigrate.foreach(flyway.setBaselineOnMigrate)
    configuration.outOfOrder.foreach(flyway.setOutOfOrder)

    flyway
  }


}
