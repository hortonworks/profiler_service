#!/usr/bin/env python

"""
Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.

Except as expressly permitted in a written agreement between you or your company
and Hortonworks, Inc. or an authorized affiliate or partner thereof, any use,
reproduction, modification, redistribution, sharing, lending or other exploitation
of all or any part of the contents of this software is strictly prohibited.
"""

import sys
from subprocess import Popen, PIPE
import logging
import time

sys.path.append('../')
from perf_utils.utils import Utils

utils = Utils()

def get_directory_size(path):
    command = ["hadoop", "fs", "-du", "-s", path]
    output = utils.run_command(command)
    words = output.split()
    if len(words) > 2:
        return words[0] + words[1]
    else:
        ##If the directory size is in bytes, it is not suffixed in the result of hdfs -du -s -h $PATH
        return words[0]

def run_profile_job(db, table):
    execution_times = []
    for num_cores in range(1, 11):
        command = ["./run_profiler_job.sh", db, table, "10g", str(num_cores)]
        logger.info("Starting profiler job for DB {0} and table {1} with {2} cores".format(db, table, num_cores))
        start_time = time.time()
        utils.run_command(command)
        end_time = time.time()
        execution_time = end_time - start_time
        logger.info("Profiler job for DB {0} and table {1} with {2} cores took {3} time".format(db, table, num_cores, execution_time))
        execution_times.append((num_cores, execution_time / 60.0))

    return execution_times

if __name__ == "__main__":
    logging.basicConfig(level= logging.DEBUG)
    logger = logging.getLogger(__name__)
    arguments = sys.argv[1:]
    if len(arguments) < 1:
        logger.error("Usage python table_size.py $DIRECTORY")
        sys.exit(0)

    directory = arguments[0]
    command = ["hadoop", "fs", "-ls", directory]
    output = utils.run_command(command)
    result_lines = output.split("\n")
    table_paths = []
    for line in result_lines[1:]:
        words = line.split()
        if len(words) > 1:
            table_paths.append(words[-1])

    logger.info("Found these HDFS paths {0}".format(table_paths))
    table_path_with_size = []

    for table_path in table_paths:
        logger.info("Computing size of HDFS path {0}".format(table_path))
        table_size = get_directory_size(table_path)
        table_name = table_path.split("/")[-1]
        table_path_with_size.append((table_name, table_size))


    logger.info("Table paths with sizes => {0}".format(table_path_with_size))
    for entry in table_path_with_size:
        print entry


    ###Temp Code to perform test run of profiler job on all tables in DB
    db = "tpcds_text_200"
    for table in table_path_with_size:
        execution_times = run_profile_job(db, table[0])
        logger.info("Table {} with size {} took times {}".format(db+"." + table[0], table[1], execution_times))