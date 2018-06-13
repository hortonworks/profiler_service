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
sys.path.append('../')
from perf_utils.utils import Utils

utils = Utils()

class HiveDestroyer:
    logger = logging.getLogger(__name__)
    def extract_query_result(self, output):
        logger.info("Extracting query results from the output => {0}".format(output))
        result_lines = output.split("\n")
        result_list = []
        if len(result_lines) > 4:
            for line in result_lines[3:-2]:
                words = line.split()
                result_entry = words[1]
                result_list.append(result_entry)

            logger.debug("Found results => {0}".format(result_list))
        else:
            logger.info("No results found in query")

        return result_list

    def get_database_list(self):
        command = ["beeline", "-u", self.hiveserver2_url, "-n", "hdfs", "-p", "hdfs", "-e", "show databases"]
        result = utils.run_command(command)
        return self.extract_query_result(result)

    def get_table_list(self, db_name):
        command = ["beeline", "-u", self.hiveserver2_url, "-n", "hdfs", "-p", "hdfs", "-e", "show tables in " + db_name]
        result = utils.run_command(command)
        return self.extract_query_result(result)

    def clean_up_tables_and_db(self, filename):
        logger.info("Executing drop commands in the file => {0}".format(filename))
        command = ["beeline", "-u", self.hiveserver2_url, "-n", "hdfs", "-p", "hdfs", "-f", filename]
        result = utils.run_command(command)
        logger.info("Cleanup query returned result => {0}".format(result))

    def __init__(self, hiveserver2_url):
        self.hiveserver2_url = hiveserver2_url


if __name__ == "__main__":
    logging.basicConfig(level= logging.DEBUG)
    logger = logging.getLogger(__name__)
    arguments = sys.argv[1:]
    if len(arguments) < 1:
        logger.error("Usage python nuke_hive_instance.py $HIVESERVER2")
        sys.exit(0)

    hiveserver2_url = arguments[0]
    jdbc_url="jdbc:hive2://" + hiveserver2_url + ":10000/default"
    hive_destroyer = HiveDestroyer(jdbc_url)
    db_list = hive_destroyer.get_database_list()
    ## Remove Database default from the list since dropping default database is not supported
    logger.debug("Removing default from the list of found databases")
    db_list.remove("default")
    filename = "drop_commands.hql"
    f = open(filename, "w")
    for db in db_list:
        table_list = hive_destroyer.get_table_list(db)
        for table in table_list:
            f.write("drop table " + db + "." + table + ";\n")
        f.write("drop database " + db + ";\n")
    f.close()
    hive_destroyer.clean_up_tables_and_db(filename)