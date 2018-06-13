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
from resource_management.core.source import Template
from subprocess import Popen, PIPE
import base64
import urllib2

class AtlasModelChanges:

    def is_model_registered(self):
        try:
            usernamepassword = '{0}:{1}'.format(self.username, self.password)
            base_64_string = base64.encodestring(usernamepassword).replace('\n', '')
            atlas_rest_endpoint = self.atlas_rest_address + "/api/atlas/v2/types/structdef/name/hive_column_profile_data"
            request = urllib2.Request(atlas_rest_endpoint)
            request.add_header("Content-Type", "application/json")
            request.add_header("Accept", "application/json")
            request.add_header("Authorization", "Basic {0}".format(base_64_string))
            result = urllib2.urlopen(request, timeout=20)
            response_code = result.getcode()
            if response_code == 200:
                return True
            else:
                return None

        except urllib2.HTTPError, e:
            return None

        except urllib2.URLError, e:
            return None

        except Exception, e:
            return None

    def add_hive_profile_types(self):
        try:
            hive_profile_types_json = Template("hive_profiler_model.json").get_content()
            atlas_rest_endpoint = self.atlas_rest_address + "/api/atlas/v2/types/typedefs"
            Logger.info("Attempting to register Hive profile types at Atlas server at {0}".format(atlas_rest_endpoint))
            Logger.debug("Payload for the profiler model registration request => {0}".format(hive_profile_types_json))
            username_password = '{0}:{1}'.format(self.username, self.password)
            base_64_string = base64.encodestring(username_password).replace('\n', '')
            request = urllib2.Request(atlas_rest_endpoint, hive_profile_types_json)
            request.add_header("Content-Type", "application/json")
            request.add_header("Accept", "application/json")
            request.add_header("Authorization", "Basic {0}".format(base_64_string))
            result = urllib2.urlopen(request, timeout=20)
            response_code = result.getcode()
            if response_code == 200:
                response_content = result.read()
                Logger.info("Profile types registered successfully")
                Logger.debug("Received content from Atlas server {0}".format(response_content))


        except urllib2.HTTPError, e:
            Logger.error("Error during profile type registration. Http status code - {0}. {1}".format(e.code, e.read()))

        except urllib2.URLError, e:
            Logger.error("Error during profile type registration. {0}".format(e.reason))

        except Exception, e:
            Logger.error("Error during profile type registration. {0}".format(e))


    def update_hive_types(self):
        try:
            updated_hive_typedefs_json = Template("typedef_update.json").get_content()
            atlas_rest_endpoint = self.atlas_rest_address + "/api/atlas/v2/types/typedefs"
            Logger.info("Attempting to update existing Hive types with profile attributes on Atlas server at {0}".format(atlas_rest_endpoint))
            Logger.debug("Payload for the Hive model update request => {0}".format(updated_hive_typedefs_json))
            username_password = '{0}:{1}'.format(self.username, self.password)
            base_64_string = base64.encodestring(username_password).replace('\n', '')
            request = urllib2.Request(atlas_rest_endpoint, updated_hive_typedefs_json)
            request.add_header("Content-Type", "application/json")
            request.add_header("Accept", "application/json")
            request.add_header("Authorization", "Basic {0}".format(base_64_string))
            request.get_method = lambda : 'PUT'
            result = urllib2.urlopen(request, timeout=20)
            response_code = result.getcode()
            if response_code == 200:
                response_content = result.read()
                Logger.info("Hive types updated with profile attributes successfully")
                Logger.debug("Received content from Atlas server {0}".format(response_content))


        except urllib2.HTTPError, e:
            Logger.error("Error during Hive type updation. Http status code - {0}. {1}".format(e.code, e.read()))

        except urllib2.URLError, e:
            Logger.error("Error during Hive type updation. {0}".format(e.reason))

        except Exception, e:
            Logger.error("Error during Hive type updation. {0}".format(e))


    def __init__(self, username, password, atlas_rest_address):
        self.username = username
        self.password = password
        self.atlas_rest_address = atlas_rest_address
