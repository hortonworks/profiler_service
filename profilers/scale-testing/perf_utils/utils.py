#!/usr/bin/env python

"""
Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.

Except as expressly permitted in a written agreement between you or your company
and Hortonworks, Inc. or an authorized affiliate or partner thereof, any use,
reproduction, modification, redistribution, sharing, lending or other exploitation
of all or any part of the contents of this software is strictly prohibited.
"""

##A utility script which contains several commonly used commands

import sys
from subprocess import Popen, PIPE
import logging
import time

class Utils:
    def run_command(self, command):
        p = Popen(command, stdout = PIPE)
        result = p.communicate()
        exitcode = p.returncode
        if exitcode < 0:
            self.logger.error("Failed to run command {0} Error => {1}".format(command, result[1]))
            return None
        else:
            return result[0]

    def run_profile_job(self, db, table):
        command = ["./run_profiler_job.sh", db, table]
        self.logger.info("Starting profiler job for DB {0} and table {1}".format(db, table))
        start_time = time.time()
        self.run_command(command)
        end_time = time.time()
        execution_time = end_time - start_time
        self.logger.info("Profiler job for DB {0} and table {1}  took {2} time".format(db, table, execution_time))

        return execution_time

    def run_profile_job_poor(self, db, table):
        command = ["./run_profiler_job_poor.sh", db, table]
        self.logger.info("Starting profiler job for DB {0} and table {1}".format(db, table))
        start_time = time.time()
        self.run_command(command)
        end_time = time.time()
        execution_time = end_time - start_time
        self.logger.info("Profiler job for DB {0} and table {1}  took {2} time".format(db, table, execution_time))

        return execution_time

    def run_profile_job_medium(self, db, table):
        command = ["./run_profiler_job_medium.sh", db, table]
        self.logger.info("Starting profiler job for DB {0} and table {1}".format(db, table))
        start_time = time.time()
        self.run_command(command)
        end_time = time.time()
        execution_time = end_time - start_time
        self.logger.info("Profiler job for DB {0} and table {1}  took {2} time".format(db, table, execution_time))

        return execution_time

    def __init__(self):
        logging.basicConfig(level= logging.DEBUG)
        self.logger = logging.getLogger(__name__)