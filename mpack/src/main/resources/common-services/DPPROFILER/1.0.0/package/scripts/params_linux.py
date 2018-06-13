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

import functools
import os

from ambari_commons.constants import AMBARI_SUDO_BINARY
from ambari_commons.os_check import OSCheck

from resource_management.libraries.functions import conf_select
from resource_management.libraries.functions import format
from resource_management.libraries.functions import get_kinit_path
from resource_management.libraries.functions import stack_select
from resource_management.libraries.functions.default import default
from resource_management.libraries.functions.expect import expect
from resource_management.libraries.functions.get_not_managed_resources import get_not_managed_resources
from resource_management.libraries.functions.is_empty import is_empty
from resource_management.libraries.resources.hdfs_resource import HdfsResource
from resource_management.libraries.script import Script
from resource_management.libraries.functions import get_user_call_output
from resource_management.libraries.functions.stack_features import check_stack_feature
from resource_management.libraries.functions import StackFeature

from status_params import *

# server configurations
java_home = config['hostLevelParams']['java_home']
ambari_cluster_name = config['clusterName']
java_version = expect("/hostLevelParams/java_version", int)
host_sys_prepped = default("/hostLevelParams/host_sys_prepped", False)

dpprofiler_hosts = default("/clusterHostInfo/dpprofiler_hosts", None)
if type(dpprofiler_hosts) is list:
  dpprofiler_host_name = dpprofiler_hosts[0]
else:
  dpprofiler_host_name = dpprofiler_hosts

config = Script.get_config()
tmp_dir = Script.get_tmp_dir()
sudo = AMBARI_SUDO_BINARY

jdk_location = config['hostLevelParams']['jdk_location']

stack_root = "/usr/dss"
dpprofiler_home_dir = '/user/profiler_agent'
dpprofiler_root = 'profiler_agent'
dpprofiler_home = format('{stack_root}/current/{dpprofiler_root}')
dpprofiler_env = config['configurations']['dpprofiler-env']
user_group = config['configurations']['cluster-env']['user_group']
dpprofiler_user = dpprofiler_env['dpprofiler.user']
dpprofiler_group = dpprofiler_env['dpprofiler.group']
dpprofiler_pid_dir = dpprofiler_env['dpprofiler.pid.dir']
dpprofiler_log_dir = dpprofiler_env['dpprofiler.log.dir']
dpprofiler_data_dir = dpprofiler_env['dpprofiler.data.dir']
dpprofiler_conf_dir = dpprofiler_env['dpprofiler.conf.dir']
logback_content = dpprofiler_env['logback.content']
dpprofiler_kerberos_principal = dpprofiler_env['dpprofiler.kerberos.principal']
dpprofiler_kerberos_keytab = dpprofiler_env['dpprofiler.kerberos.keytab']
dpprofiler_spnego_kerberos_principal = dpprofiler_env['dpprofiler.spnego.kerberos.principal']
dpprofiler_spnego_kerberos_keytab = dpprofiler_env['dpprofiler.spnego.kerberos.keytab']
dpprofiler_http_port = dpprofiler_env['dpprofiler.http.port']

dpprofiler_config = config['configurations']['dpprofiler-config']
dpprofiler_db_type = dpprofiler_config["dpprofiler.db.type"]
dpprofiler_db_slick_driver = dpprofiler_config["dpprofiler.db.slick.driver"]
dpprofiler_db_driver = dpprofiler_config["dpprofiler.db.driver"]
dpprofiler_db_jdbc_url = dpprofiler_config["dpprofiler.db.jdbc.url"]
dpprofiler_db_user = dpprofiler_config["dpprofiler.db.user"]
dpprofiler_db_password = dpprofiler_config["dpprofiler.db.password"]
dpprofiler_spnego_signature_secret = dpprofiler_config["dpprofiler.spnego.signature.secret"]
dpprofiler_spnego_cookie_name = dpprofiler_config["dpprofiler.spnego.cookie.name"]

dpprofiler_profiler_autoregister = dpprofiler_config["dpprofiler.profiler.autoregister"]
dpprofiler_profiler_hdfs_dir = dpprofiler_config["dpprofiler.profiler.hdfs.dir"]
dpprofiler_profiler_dwh_dir = dpprofiler_config["dpprofiler.profiler.dwh.dir"]
dpprofiler_profiler_dir = dpprofiler_config["dpprofiler.profiler.dir"]
dpprofiler_job_status_refresh_interval = dpprofiler_config["dpprofiler.job.status.refresh.interval"]

dpprofiler_refreshtable_cron = dpprofiler_config["dpprofiler.refreshtable.cron"]
dpprofiler_refreshtable_retry = dpprofiler_config["dpprofiler.refreshtable.retry"]
dpprofiler_kerberos_ticket_refresh_cron = dpprofiler_env["dpprofiler.kerberos.ticket.refresh.cron"]
dpprofiler_kerberos_ticket_refresh_retry_allowed = dpprofiler_env["dpprofiler.kerberos.ticket.refresh.retry.allowed"]


dpprofiler_submitter_batch_size = dpprofiler_config["dpprofiler.submitter.batch.size"]
dpprofiler_submitter_jobs_max = dpprofiler_config["dpprofiler.submitter.jobs.max"]
dpprofiler_submitter_jobs_scan_seconds = dpprofiler_config["dpprofiler.submitter.jobs.scan.seconds"]
dpprofiler_submitter_queue_size = dpprofiler_config["dpprofiler.submitter.queue.size"]

dpprofiler_sensitivepartitioned_metric_name = dpprofiler_config["dpprofiler.sensitivepartitioned.metric.name"]
dpprofiler_senstitive_metric_name = dpprofiler_config["dpprofiler.senstitive.metric.name"]

dpprofiler_profiler_autoregister = "true"

livy_session_config = dpprofiler_config["livy.session.config"]

dpprofiler_custom_config = dpprofiler_config["dpprofiler.custom.config"]

if not dpprofiler_config["dpprofiler.profiler.autoregister"]:
  dpprofiler_profiler_autoregister = "false"

dpprofiler_crypto_secret = \
  get_user_call_output.get_user_call_output(format("date +%s | sha256sum | base64 | head -c 32 "),
                                            user=dpprofiler_user,
                                            is_checked_call=False)[1]

livy_hosts = default("/clusterHostInfo/livy2_server_hosts", [])

livy_url = ""

if stack_version_formatted and check_stack_feature(StackFeature.SPARK_LIVY, stack_version_formatted) and \
    len(livy_hosts) > 0:
  livy_livyserver_host = str(livy_hosts[0])
  livy_livyserver_port = config['configurations']['livy2-conf']['livy.server.port']
  livy_url = "http://" + livy_livyserver_host + ":" + livy_livyserver_port

dpprofiler_secured = "false"
if config['configurations']['cluster-env']['security_enabled']:
  dpprofiler_secured = "true"

dpprofiler_cluster_config_keys = dpprofiler_config["dpprofiler.cluster.config.keys"].split(";")
dpprofiler_cluster_configs = "isSecured=\"" + dpprofiler_secured + "\"\n"

for config_key in dpprofiler_cluster_config_keys:
  config_name = config_key.split("=")[0]
  config_file = config_key.split("=")[1].split("/")[0]
  config_key = config_key.split("=")[1].split("/")[1]
  if config_file in config['configurations'] and config_key in config['configurations'][config_file]:
    config_string = config_name + "=\"" + config['configurations'][config_file][config_key] + "\"\n"
    dpprofiler_cluster_configs += config_string

hdfs_user = config['configurations']['hadoop-env']['hdfs_user']
security_enabled = config['configurations']['cluster-env']['security_enabled']
hdfs_user_keytab = config['configurations']['hadoop-env']['hdfs_user_keytab']
kinit_path_local = get_kinit_path(default('/configurations/kerberos-env/executable_search_paths', None))
hadoop_bin_dir = stack_select.get_hadoop_dir("bin")
hadoop_conf_dir = conf_select.get_hadoop_conf_dir()
hdfs_principal_name = config['configurations']['hadoop-env']['hdfs_principal_name']
hdfs_site = config['configurations']['hdfs-site']
default_fs = config['configurations']['core-site']['fs.defaultFS']

atlas_rest_address = ""
atlas_username = ""
atlas_password = ""

if 'application-properties' in config['configurations']:
  atlas_rest_address = config['configurations']['application-properties']['atlas.rest.address']

if 'atlas-env' in config['configurations']:
  atlas_username = config['configurations']['atlas-env']["atlas.admin.username"]
  atlas_password = config['configurations']['atlas-env']["atlas.admin.password"]

ranger_username = ""
ranger_password = ""
ranger_audit_hdfs = ""
ranger_audit_hdfs_dir = ""
ranger_url = ""

if 'ranger-env' in config['configurations']:
  ranger_username = config['configurations']['ranger-env']['admin_username']
  ranger_password = config['configurations']['ranger-env']['admin_password']
  ranger_audit_hdfs = config['configurations']['ranger-env']['xasecure.audit.destination.hdfs']
  ranger_audit_hdfs_dir = config['configurations']['ranger-env']['xasecure.audit.destination.hdfs.dir']
  ranger_url = config['configurations']['admin-properties']['policymgr_external_url']

# create partial functions with common arguments for every HdfsResource call
# to create hdfs directory we need to call params.HdfsResource in code
HdfsResource = functools.partial(
  HdfsResource,
  user=hdfs_user,
  hdfs_resource_ignore_file="/var/lib/ambari-agent/data/.hdfs_resource_ignore",
  security_enabled=security_enabled,
  keytab=hdfs_user_keytab,
  kinit_path_local=kinit_path_local,
  hadoop_bin_dir=hadoop_bin_dir,
  hadoop_conf_dir=hadoop_conf_dir,
  principal_name=hdfs_principal_name,
  hdfs_site=hdfs_site,
  default_fs=default_fs
)


# mysql driver download properties
patch_mysql_driver = dpprofiler_db_driver == "com.mysql.jdbc.Driver"
jdk_location = config['hostLevelParams']['jdk_location']
jdbc_jar_name = default("/hostLevelParams/custom_mysql_jdbc_name", None)
driver_source = format("{jdk_location}/{jdbc_jar_name}")
mysql_driver_target = os.path.join(dpprofiler_home, "lib/mysql-connector-java.jar")