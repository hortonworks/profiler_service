#!/usr/bin/env python

"""
Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.

Except as expressly permitted in a written agreement between you or your company
and Hortonworks, Inc. or an authorized affiliate or partner thereof, any use,
reproduction, modification, redistribution, sharing, lending or other exploitation
of all or any part of the contents of this software is strictly prohibited.
"""

import string
import random
import sys
import logging

class TableGenerator:
    col_type_list = ["int", "string", "boolean", "decimal(10,3)", "bigint", "float", "double", "smallint"]
    boolean_values = ["true", "false", "NULL"]
    def get_table_name(self, size=10):
        return random.choice(string.ascii_uppercase) + ''.join(random.choice(string.ascii_letters) for _ in range(size))

    def get_column_name(self, size=6):
        return  ''.join(random.choice(string.ascii_letters) for _ in range(size))

    def column_list(self, count = 10):
        col_list = []
        for i in xrange(count):
            index = random.randint(0, len(self.col_type_list) -1)
            col_type = self.col_type_list[index]
            col_name = self.get_column_name()
            col_list.append((col_name, col_type))
        return col_list

    def construct_query(self, table_name, column_list):
        query = "CREATE TABLE IF NOT EXISTS `default." + table_name + "`("
        for entry in column_list[:-1]:
            query +=  "`" + entry[0] + "` " + entry[1] + ","

        last_entry =column_list[-1]
        query += "`" + last_entry[0] + "` " + last_entry[1] + ");"
        return query

    def random_col_value(self, column_type):
        if column_type in ["int", "bigint", "smallint"]:
            return random.randint(1, 1000000)
        elif column_type in ["float", "decimal(10,3)", "double"]:
            return str(random.randint(1, 1000000)) + "." + str(random.randint(1, 100))
        elif column_type == "string":
            return ''.join(random.choice(string.ascii_letters) for _ in range(random.randint(1, 1000)))
        elif column_type == "boolean":
            index = random.randint(0, 2)
            return self.boolean_values[index]
        else:
            return "NULL"

    def value_string(self, value):
        return '"' + str(value) + '"'

    def construct_data_load_query(self, table_name, column_list, number_of_rows):
        query = "INSERT INTO TABLE `default." + table_name + "` values "
        value_str_list = []
        for i in xrange(number_of_rows):
            random_values = list(map(self.random_col_value, list(zip(*column_list)[1])))
            random_values_str = ','.join(map(self.value_string, random_values))
            value_str_list.append("(" + random_values_str + ")")

        query += ", ".join(value_str_list) + ";"
        return query

    def __init__(self):
        logging.basicConfig(level= logging.DEBUG)
        self.logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logging.basicConfig(level= logging.DEBUG)
    logger = logging.getLogger(__name__)
    arguments = sys.argv[1:]
    if len(arguments) < 1:
        logger.error("Usage python generate_tables.py [NUM_TABLES] [NUM_COLS] [NUM_ROWS]")
        sys.exit(0)
    number_of_tables = int(arguments[0])
    number_of_columns = int(arguments[1])
    number_of_rows = int(arguments[2])
    table_generator = TableGenerator()
    f = open("tableCreationQueries.hql", "w")
    for i in range(number_of_tables):
        table_name = table_generator.get_table_name()
        col_list = table_generator.column_list(number_of_columns)
        f.write(table_generator.construct_query(table_name, col_list) + "\n")
        f.write(table_generator.construct_data_load_query(table_name, col_list, number_of_rows) + "\n")
        logger.info("Table {0} with {1} columns and {2} rows has been generated".format(table_name, number_of_columns, number_of_rows))
    f.close()