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

import base64
import ssl
import urllib2
from resource_management.core.logger import Logger

redirect_codes={307,303,302}
class AtlasRequestHandler:
    def __init__(self, url_list, atlas_username, atlas_password, timeout=50):
        username_password = '{0}:{1}'.format(atlas_username, atlas_password)
        base_64_string = base64.encodestring(username_password).replace('\n', '')
        self.headers = {"Content-Type": "application/json", "Accept": "application/json", \
                        "Authorization": "Basic {0}".format(base_64_string)}
        self.url_list = url_list
        self.timeout = timeout

    def handle_request(self, api_endpoint, payload=None, is_put=False):
        for url in self.url_list:
            try:
                Logger.info("Trying to connect to Atlas at  {}".format(url))
                request = urllib2.Request(url + api_endpoint, payload, self.headers)
                request_context = None
                if url.startswith("https"):
                    request_context = ssl._create_unverified_context()
                if is_put:
                    request.get_method = lambda: 'PUT'
                result = urllib2.urlopen(request, timeout=self.timeout, context=request_context)
                if result.code not in redirect_codes:
                    return result.code, result.read()
                else:
                    Logger.info("Received redirect code {} from {}".format(result.code, url))

            except urllib2.HTTPError, e:
                Logger.error("HTTP Error while handling the request - {0}. {1}".format(e.code, e.read()))
                if e.code not in redirect_codes:
                    return e.code, e.read()
                else:
                    Logger.info("Received redirect code {} from {}".format(e.code, url))

            except urllib2.URLError, e:
                Logger.error("URL Error {0} while handling request. Trying another URL".format(e.reason))

            except Exception, e:
                Logger.error("Got Exception {0} while handling the request. Trying another URL".format(e))

        return None, None
