#!/usr/bin/env python

"""
Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.

Except as expressly permitted in a written agreement between you or your company
and Hortonworks, Inc. or an authorized affiliate or partner thereof, any use,
reproduction, modification, redistribution, sharing, lending or other exploitation
of all or any part of the contents of this software is strictly prohibited.
"""

import sys
import logging
from utils import Utils

utils = Utils()

###Perform runs of the same table in various size of TPC text dataset
def same_table_diff_text_db_runs(table_name):
    db_list = ["tpcds_text_2", "tpcds_text_5", "tpcds_text_10", "tpcds_text_20", "tpcds_text_50", "tpcds_text_100"]
    for db in db_list:
        execution_time = utils.run_profile_job(db, table_name)
        print "DB", db, " Table ", table_name, " took time ", execution_time

###Perform runs of the same table in various size of TPC ORC dataset
def same_table_diff_orc_db_runs(table_name):
    db_list = ["tpcds_bin_partitioned_orc_2", "tpcds_bin_partitioned_orc_5", "tpcds_bin_partitioned_orc_10", "tpcds_bin_partitioned_orc_20", "tpcds_bin_partitioned_orc_50", "tpcds_bin_partitioned_orc_100"]
    for db in db_list:
        execution_time = utils.run_profile_job(db, table_name)
        print "DB", db, " Table ", table_name, " took time ", execution_time


###Perform runs of tables with different number of columns
def wide_column_runs():
    db = "default"
    table_list = [('QkekrAAORVe', '1'), ('UrKzpYWPnYx', '10'), ('KjeGNjIYXOY', '50'), ('EVCrVatTlIK', '100'), ('NpdBcKduURv', '200'), ('FXddldNEoIl', '500')]
    for entry in table_list:
        table_name = entry[0]
        num_cols = entry[1]
        execution_time = utils.run_profile_job(db, table_name)
        print "Table ", table_name, " with num_columns ", num_cols, " took time ", execution_time

###Perform runs for different sized table in the same DB
def same_db_different_table_runs():
    db = ["tpcds_bin_partitioned_orc_1000", "tpcds_text_1000"]
    table_list = [('catalog_page', '4M'), ('customer_demographics', '80M'), ('customer_address', '650MB'), ('customer', '1.5G'), ('web_returns', '10G'), ('inventory', '16 G'), ('catalog_returns', '22G'), ('store_returns', '33 G'), ('web_sales', '150G'), ('catalog_sales', '300G'), ('store_sales', '400G')]
    for entry in table_list:
        for db_name in db:
            table_name = entry[0]
            table_size = entry[1]
            execution_time = utils.run_profile_job(db_name, table_name)
            print "DB", db_name, " Table ", table_name, " Size ", table_size, " took time ", execution_time

if __name__ == "__main__":
    print "Uncomment one of the functions below to perform test runs appropriately"
    #same_db_different_table_runs()
    #wide_column_runs()
    #same_table_diff_text_db_runs("store_sales")
    #same_table_diff_orc_db_runs("store_sales")