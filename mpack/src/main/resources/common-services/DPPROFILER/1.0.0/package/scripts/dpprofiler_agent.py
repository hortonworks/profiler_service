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

import glob
import os
from resource_management.core.logger import Logger
from resource_management.core.resources import Directory
from resource_management.core.resources.system import Execute, File
from resource_management.core.source import InlineTemplate
from resource_management.core.source import Template
from resource_management.libraries.functions import StackFeature
from resource_management.libraries.functions import conf_select
from resource_management.libraries.functions import stack_select
from resource_management.libraries.functions.check_process_status import check_process_status
from resource_management.libraries.functions.format import format
from resource_management.libraries.functions.stack_features import check_stack_feature
from resource_management.libraries.functions.version import format_stack_version
from resource_management.libraries.script.script import Script
from resource_management.libraries.functions import get_user_call_output
from resource_management.libraries.functions.show_logs import show_logs
from resource_management.core.exceptions import Fail
from resource_management.core.source import DownloadSource
from urlparse import urlparse

class DpProfilerAgent(Script):
  def get_component_name(self):
    return "profiler_agent"

  def install(self, env):
    import params
    env.set_params(params)
    self.install_packages(env)

    Directory([params.dpprofiler_pid_dir],
              owner=params.dpprofiler_user,
              group=params.dpprofiler_group,
              cd_access="a",
              create_parents=True,
              mode=0755
              )

    Directory([params.dpprofiler_profiler_dir],
              owner=params.dpprofiler_user,
              group=params.dpprofiler_group,
              cd_access="a",
              create_parents=True,
              mode=0755
              )

    Directory([params.dpprofiler_data_dir],
              owner=params.dpprofiler_user,
              group=params.dpprofiler_group,
              cd_access="a",
              create_parents=True,
              mode=0755
              )

    # update the configs specified by user
    #self.configure(env)

  def create_dpprofiler_dir(self, params):
    Logger.info("Configuring dpprofiler hdfs directory")

    params.HdfsResource(format("/user/{dpprofiler_user}"),
                        type="directory",
                        action="create_on_execute",
                        owner=params.dpprofiler_user,
                        recursive_chown=False,
                        recursive_chmod=False
                        )

    params.HdfsResource(format(params.dpprofiler_profiler_hdfs_dir),
                        type="directory",
                        action="create_on_execute",
                        owner=params.dpprofiler_user,
                        recursive_chown=False,
                        recursive_chmod=False
                        )

    params.HdfsResource(format(params.dpprofiler_profiler_dwh_dir),
                        type="directory",
                        action="create_on_execute",
                        owner=params.dpprofiler_user,
                        recursive_chown=False,
                        recursive_chmod=False
                        )

  def create_dpprofiler_log_dir(self, env):
    import params
    Logger.info("Configuring log directory")
    env.set_params(params)
    Directory([params.dpprofiler_log_dir],
              owner=params.dpprofiler_user,
              group=params.dpprofiler_group,
              cd_access="a",
              create_parents=True,
              mode=0755
              )

  def create_profile_types_atlas(self):
      import params
      from atlas_model_update import AtlasModelChanges
      atlas_model_obj = AtlasModelChanges(params.atlas_username, params.atlas_password, params.atlas_rest_address)
      Logger.info("Got Atlas URL => " + params.atlas_rest_address)
      Logger.info("Got Atlas Credentials => Username : " + params.atlas_username)
      if not atlas_model_obj.is_model_registered():
          Logger.info("Atlas profile model is not registered. Attempting to register the profile model ...")
          atlas_model_obj.add_hive_profile_types()
          atlas_model_obj.update_hive_types()

      else:
          Logger.info("Atlas profiler model is already registered. Skipping")


  def create_ranger_policy(self, env):
    import params
    from ranger_policy_update import RangerPolicyUpdate

    if params.ranger_audit_hdfs and params.ranger_password and params.ranger_url and params.ranger_username:
      parsed_url = urlparse(params.ranger_audit_hdfs_dir)
      ranger_audit_path = parsed_url.path
      RangerPolicyUpdate(params.ranger_url, params.ranger_username, params.ranger_password, ranger_audit_path,
                         params.dpprofiler_user, env).create_policy_if_needed()
    else:
      Logger.info("Skipping ranger policy update.")

  def configure(self, env):
    import params
    import status_params
    env.set_params(params)
    env.set_params(status_params)
    self.create_profile_types_atlas()
    self.create_ranger_policy(env)
    self.create_dpprofiler_log_dir(env)
    self.create_dpprofiler_dir(params)

    Directory([params.dpprofiler_conf_dir],
              owner='root',
              group='root',
              cd_access="a",
              create_parents=True,
              mode=0755
              )

    Logger.info("Writing conf file")

    File(os.path.join(params.dpprofiler_conf_dir, 'application.conf'),
         owner=params.dpprofiler_user,
         group=params.dpprofiler_group,
         mode=0600,
         content=Template("application.conf.j2")
         )

    File(os.path.join(params.dpprofiler_conf_dir, 'flyway.conf'),
         owner=params.dpprofiler_user,
         group=params.dpprofiler_group,
         mode=0600,
         content=Template("flyway.conf.j2")
         )

    File(os.path.join(params.dpprofiler_conf_dir, 'clusterconfigs.conf'),
         owner=params.dpprofiler_user,
         group=params.dpprofiler_group,
         mode=0600,
         content=Template("clusterconfigs.conf.j2")
         )

    if params.dpprofiler_secured:
      File(os.path.join(params.dpprofiler_conf_dir, 'krb5JAASLogin.conf'),
           owner=params.dpprofiler_user,
           group=params.dpprofiler_group,
           mode=0600,
           content=Template("krb5JAASLogin.conf.j2")
           )

    # write out logback.xml
    logback_content = InlineTemplate(params.logback_content)
    File(format("{params.dpprofiler_conf_dir}/logback.xml"), content=logback_content,
         owner=params.dpprofiler_user, group=params.dpprofiler_group)

  def stop(self, env, upgrade_type=None):
    import params
    self.configure(env)

    pid = get_user_call_output.get_user_call_output(format("cat {dpprofiler_pid_file}"), user=params.dpprofiler_user,
                                                    is_checked_call=False)[1]
    process_exists = format("ls {dpprofiler_pid_file} && ps -p {pid}")

    daemon_kill_cmd = format("{sudo} kill {pid}")
    daemon_hard_kill_cmd = format("{sudo} kill -9 {pid}")

    Execute(daemon_kill_cmd,
            not_if=format("! ({process_exists})")
            )

    wait_time = 5
    Execute(daemon_hard_kill_cmd,
            not_if=format("! ({process_exists}) || ( sleep {wait_time} && ! ({process_exists}) )"),
            ignore_failures=True
            )

    try:
      # check if stopped the process, else fail the task
      Execute(format("! ({process_exists})"),
              tries=20,
              try_sleep=3,
              )
    except:
      show_logs(params.dpprofiler_log_dir, params.dpprofiler_user)
      raise

    File(params.dpprofiler_pid_file,
         action="delete"
         )

  def patch_mysql_driver(self):
    import params

    Logger.info("Patching mysql driver")

    Logger.info("Mysql Jar source :" + params.driver_source)
    Logger.info("Mysql Jar target :" + params.mysql_driver_target)

    if params.jdbc_jar_name is None:
      raise Fail("Mysql JDBC driver not installed on ambari-server")

    File(
      params.mysql_driver_target,
      content=DownloadSource(params.driver_source),
      mode=0644
    )

    start_script = params.dpprofiler_home + "/bin/profiler-agent"

    Logger.info("Updating script : " + start_script)

    search_string = "declare -r app_classpath=\""
    replace_string = "declare -r app_classpath=\"" + params.mysql_driver_target + ":"

    data = open(start_script).read()
    if replace_string not in data :
      Logger.info("Adding mysql in classpath")
      data = data.replace(search_string, replace_string)
      f = open(start_script, 'w')
      f.write(data)
      f.close()



  def start(self, env, upgrade_type=None):
    import params
    self.configure(env)

    Logger.info("Configured Dirs")

    pid = get_user_call_output.get_user_call_output(format("cat {dpprofiler_pid_file}"), user=params.dpprofiler_user,
                                                    is_checked_call=False)[1]
    process_exists = format("ls {dpprofiler_pid_file} && ps -p {pid}")

    Logger.info("Starting profiler agent")
    environment_dictionary = {}
    environment_dictionary["DPPROFILER_CRYPTO_SECRET"] = params.dpprofiler_crypto_secret

    kerberos_props = ''
    if params.dpprofiler_secured == "true":
      kerberos_props = format(
        '-Djava.security.krb5.conf=/etc/krb5.conf -Djavax.security.auth.useSubjectCredsOnly=false -Djava.security.auth.login.config={dpprofiler_conf_dir}/krb5JAASLogin.conf')

    Execute(format('rm -f {params.dpprofiler_pid_file}'),
            not_if=process_exists
            )

    if params.patch_mysql_driver:
      self.patch_mysql_driver()

    Execute(format(
      'nohup {dpprofiler_home}/bin/profiler-agent -Dhttp.port={dpprofiler_http_port} {kerberos_props} > {dpprofiler_log_dir}/profiler_agent.out 2>&1 &'),
      user=params.dpprofiler_user,
      not_if=process_exists,
      environment=environment_dictionary
    )

    try:
      # check if pid file created, else fail the task
      Execute(format("(ls {dpprofiler_pid_file})"),
              tries=20,
              try_sleep=3,
              )
    except:
      show_logs(params.dpprofiler_log_dir, params.dpprofiler_user)
      raise

    newpid = get_user_call_output.get_user_call_output(format("cat {dpprofiler_pid_file}"), user=params.dpprofiler_user,
                                                       is_checked_call=False)[1]

    Logger.info(format("Process pid is: {newpid}"))

  def status(self, env):
    import status_params
    env.set_params(status_params)

    try:
      pid_file = glob.glob(status_params.dpprofiler_pid_file)[0]
    except IndexError:
      pid_file = ''
    check_process_status(pid_file)


  def pre_upgrade_restart(self, env, upgrade_type=None):
    pass


if __name__ == "__main__":
  DpProfilerAgent().execute()
