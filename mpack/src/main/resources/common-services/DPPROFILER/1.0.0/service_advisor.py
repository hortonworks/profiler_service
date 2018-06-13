#!/usr/bin/env ambari-python-wrap
"""
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
"""

import os
import fnmatch
import imp
import socket
import sys
import traceback

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
STACKS_DIR = os.path.join(SCRIPT_DIR, '../../../../../stacks/')
PARENT_FILE = os.path.join(STACKS_DIR, 'service_advisor.py')

try:
  with open(PARENT_FILE, 'rb') as fp:
    service_advisor = imp.load_module('service_advisor', fp, PARENT_FILE, ('.py', 'rb', imp.PY_SOURCE))
except Exception as e:
  traceback.print_exc()
  print "Failed to load parent"


class DPPROFILER100ServiceAdvisor(service_advisor.ServiceAdvisor):
  def __init__(self, *args, **kwargs):
    self.as_super = super(DPPROFILER100ServiceAdvisor, self)
    self.as_super.__init__(*args, **kwargs)

  def getServiceComponentLayoutValidations(self, services, hosts):

    componentsListList = [service["components"] for service in services["services"]]
    componentsList = [item["StackServiceComponents"] for sublist in componentsListList for item in sublist]

    items = []
    return items

  def getServiceConfigurationRecommendations(self, configurations, clusterData, services, hosts):

    putDPProfilerConfigProperty = self.putProperty(configurations, 'dpprofiler-config', services)

    if 'forced-configurations' not in services:
      services["forced-configurations"] = []

    if ('dpprofiler-config' in services['configurations']) and (
      'dpprofiler.db.type' in services['configurations']['dpprofiler-config']['properties']) \
      and ('dpprofiler.db.driver' in services['configurations']['dpprofiler-config']['properties']):

      dpprofiler_data_dir = services['configurations']['dpprofiler-env']['properties']['dpprofiler.data.dir']
      database_type = services['configurations']['dpprofiler-config']['properties']['dpprofiler.db.type']
      putDPProfilerConfigProperty('dpprofiler.db.driver', self.getDpProfilerDBDriver(database_type))
      putDPProfilerConfigProperty('dpprofiler.db.slick.driver', self.getDpProfilerDBSlickDriver(database_type))

      if ('dpprofiler.db.database' in services['configurations']['dpprofiler-config']['properties']) \
        and ('dpprofiler.db.jdbc.url' in services['configurations']['dpprofiler-config']['properties']):
        dpprofiler_db_connection_url = services['configurations']['dpprofiler-config']['properties'][
          'dpprofiler.db.jdbc.url']
        dpprofiler_db_database = services['configurations']['dpprofiler-config']['properties']['dpprofiler.db.database']
        dpprofiler_db_host = services['configurations']['dpprofiler-config']['properties']['dpprofiler.db.host']
        protocol = self.getDBProtocol(database_type)
        old_schema_name = self.getOldPropertyValue(services, 'dpprofiler-config', 'dpprofiler.db.database')
        old_db_type = self.getOldPropertyValue(services, 'dpprofiler-config', 'dpprofiler.db.type')
        old_host = self.getOldPropertyValue(services, 'dpprofiler-config', 'dpprofiler.db.host')

        # if it's default db connection url with "localhost" or if schema name was changed or if db type was changed (only for db type change from default mysql to existing mysql)
        # or if protocol according to current db type differs with protocol in db connection url(other db types changes)
        if (dpprofiler_db_connection_url and "//localhost" in dpprofiler_db_connection_url) \
          or old_schema_name or old_db_type or old_host or (protocol and dpprofiler_db_connection_url \
                                                              and not dpprofiler_db_connection_url.startswith(
            protocol)):
          db_connection = self.getDpProfilerDBConnectionString(database_type).format(dpprofiler_db_host,
                                                                                     dpprofiler_db_database,
                                                                                     dpprofiler_data_dir)
          putDPProfilerConfigProperty('dpprofiler.db.jdbc.url', db_connection)

  def getOldPropertyValue(self, services, configType, propertyName):
    if services:
      if 'changed-configurations' in services.keys():
        changedConfigs = services["changed-configurations"]
        for changedConfig in changedConfigs:
          if changedConfig["type"] == configType and changedConfig[
            "name"] == propertyName and "old_value" in changedConfig:
            return changedConfig["old_value"]
    return None

  def getDpProfilerDBDriver(self, databaseType):
    driverDict = {
      'mysql': 'com.mysql.jdbc.Driver',
      'h2': 'org.h2.Driver',
      'postgres': 'org.postgresql.Driver'
    }
    return driverDict.get(databaseType)

  def getDpProfilerDBSlickDriver(self, databaseType):
    driverDict = {
      'h2': 'slick.driver.H2Driver$',
      'mysql': 'slick.driver.MySQLDriver$',
      'postgres': 'slick.driver.PostgresDriver$'
    }
    return driverDict.get(databaseType)

  def getDpProfilerDBConnectionString(self, databaseType):
    driverDict = {
      'h2': 'jdbc:h2:{2}/h2/profileragent;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1',
      'mysql': 'jdbc:mysql://{0}:3306/{1}?autoreconnect=true',
      'postgres': 'jdbc:postgres://{0}:5432/{1}'
    }
    return driverDict.get(databaseType)

  def getDBProtocol(self, databaseType):
    first_parts_of_connection_string = {
      'mysql': 'jdbc:mysql',
      'h2': 'jdbc:derby',
      'postgres': 'jdbc:postgresql'
    }
    return first_parts_of_connection_string.get(databaseType)
