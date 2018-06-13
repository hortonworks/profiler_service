#!/usr/bin/env python
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

from resource_management.core.logger import Logger
import base64
import urllib2
import ambari_simplejson as json
from resource_management.core.source import Template


class RangerPolicyUpdate:
  def create_policy_if_needed(self):
    service_name = self.get_service()
    Logger.info("Ranger Hdfs service name : {0}".format(service_name))
    if service_name:
      if self.check_if_not_policy_exist(service_name):
        self.create_policy(service_name)
      else:
        Logger.info("Policy already exists.")
    else:
      Logger.error("Ranger hdfs service not found")

  def get_service(self):
    try:
      url = self.rangerurl + "/service/public/v2/api/service?serviceType=hdfs"
      Logger.info("Getting ranger service name for hdfs. Url : {0}".format(url))
      usernamepassword = '{0}:{1}'.format(self.username, self.password)
      base_64_string = base64.encodestring(usernamepassword).replace('\n', '')
      request = urllib2.Request(url)
      request.add_header("Content-Type", "application/json")
      request.add_header("Accept", "application/json")
      request.add_header("Authorization", "Basic {0}".format(base_64_string))
      result = urllib2.urlopen(request, timeout=20)
      response_code = result.getcode()
      if response_code == 200:
        response = json.loads(result.read())
        if (len(response) > 0):
          return response[0]['name']
        else:
          return None

    except urllib2.HTTPError, e:
      Logger.error(
        "Error during Ranger service authentication. Http status code - {0}. {1}".format(e.code, e.read()))
      return None

    except urllib2.URLError, e:
      Logger.error("Error during Ranger service authentication. {0}".format(e.reason))
      return None
    except Exception, e:
      Logger.error("Error occured when connecting Ranger admin. {0} ".format(e))
      return None

  def check_if_not_policy_exist(self, service_name):
    try:
      url = self.rangerurl + "/service/public/v2/api/service/" + \
            service_name + "/policy?policyName=dpprofiler-audit-read"
      Logger.info("Checking ranger policy. Url : {0}".format(url))
      usernamepassword = '{0}:{1}'.format(self.username, self.password)
      base_64_string = base64.encodestring(usernamepassword).replace('\n', '')
      request = urllib2.Request(url)
      request.add_header("Content-Type", "application/json")
      request.add_header("Accept", "application/json")
      request.add_header("Authorization", "Basic {0}".format(base_64_string))
      result = urllib2.urlopen(request, timeout=20)
      response_code = result.getcode()
      if response_code == 200:
        response = json.loads(result.read())
        return (len(response) == 0)

    except urllib2.HTTPError, e:
      Logger.error(
        "Error during Ranger service authentication. Http status code - {0}. {1}".format(e.code, e.read()))
      return False

    except urllib2.URLError, e:
      Logger.error("Error during Ranger service authentication. {0}".format(e.reason))
      return False
    except Exception, e:
      Logger.error("Error occured when connecting Ranger admin. {0}".format(e))
      return False

  def create_policy(self, service_name):
    try:
      variable = {
        'ranger_audit_hdfs_path': self.ranger_audit_hdfs_path,
        'dpprofiler_user': self.dpprofiler_user,
        'service_name': service_name
      }
      self.env.set_params(variable)

      data = Template("dpprofiler_ranger_policy.json").get_content()

      url = self.rangerurl + "/service/public/v2/api/policy"
      Logger.info("Creating ranger policy. Url : {0}".format(url))
      Logger.info("data: {0}".format(data))
      usernamepassword = '{0}:{1}'.format(self.username, self.password)
      base_64_string = base64.encodestring(usernamepassword).replace('\n', '')
      request = urllib2.Request(url, data)
      request.add_header("Content-Type", "application/json")
      request.add_header("Accept", "application/json")
      request.add_header("Authorization", "Basic {0}".format(base_64_string))
      result = urllib2.urlopen(request, timeout=20)
      response_code = result.getcode()
      Logger.info("Response code for create policy : {0}".format(response_code))
      if response_code == 200:
        response = json.loads(result.read())
        return response

    except urllib2.HTTPError, e:
      Logger.error(
        "Error during Ranger service authentication. Http status code - {0}. {1}".format(e.code, e.read()))
      return None

    except urllib2.URLError, e:
      Logger.error("Error during Ranger service authentication. {0}".format(e.reason))
      return None
    except Exception, e:
      Logger.error("Error occured when connecting Ranger admin. {0}".format(e))
      return None

  def __init__(self, rangerurl, username, password, ranger_audit_hdfs_path, dpprofiler_user, env):
    self.rangerurl = rangerurl
    self.username = username
    self.password = password
    self.ranger_audit_hdfs_path = ranger_audit_hdfs_path
    self.dpprofiler_user = dpprofiler_user
    self.env = env
