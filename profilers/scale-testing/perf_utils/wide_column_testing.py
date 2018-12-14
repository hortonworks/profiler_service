#!/usr/bin/env python

"""
Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.

Except as expressly permitted in a written agreement between you or your company
and Hortonworks, Inc. or an authorized affiliate or partner thereof, any use,
reproduction, modification, redistribution, sharing, lending or other exploitation
of all or any part of the contents of this software is strictly prohibited.
"""
from utils import Utils
import logging

if __name__ == "__main__":
    num_column_arr = [1, 10, 50, 100, 200, 500, 700, 1000]
    num_row_arr = [1, 10, 50, 100, 500, 1000, 2000, 5000, 10000]
    util = Utils()
    logging.basicConfig(level= logging.DEBUG)
    logger = logging.getLogger(__name__)
    hiveserver2_address = "profiler-perf-2.openstacklocal"

    for num_column in num_column_arr:
        command = ["./create_hive_tables.sh", hiveserver2_address, "1", str(num_column), "1000"]
        result = util.run_command(command)
        logger.info("Got result {0}".format(result))


    for num_rows in num_row_arr:
        command = ["./create_hive_tables.sh", hiveserver2_address, "1", "1000", str(num_rows)]
        result = util.run_command(command)
        logger.info("Got result {0}".format(result))
        