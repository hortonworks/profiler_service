#!/usr/bin/env python

"""
Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.

Except as expressly permitted in a written agreement between you or your company
and Hortonworks, Inc. or an authorized affiliate or partner thereof, any use,
reproduction, modification, redistribution, sharing, lending or other exploitation
of all or any part of the contents of this software is strictly prohibited.
"""
import logging
import random
import string
import sys
class SensitiveTableGenerator:

    def get_random_name(self, size=10):
        return random.choice(string.ascii_uppercase) + ''.join(random.choice(string.ascii_letters) for _ in range(size))

    def create_db_and_table_queries(self, max_tables):
        db_name = self.get_random_name()
        queries = "CREATE DATABASE IF NOT EXISTS " + db_name + ";\n"
        num_tables = random.randint(5, max_tables)
        for i in range(num_tables):
            num_rows = random.randint(100, 50000)
            table_name = self.get_random_name()
            queries += "CREATE TABLE " + db_name + "." + table_name + " LIKE hortoniabank.us_customers;\n"
            queries += "INSERT INTO " + db_name + "." + table_name + " SELECT * FROM hortoniabank.us_customers LIMIT " + \
                       str(num_rows) + ";\n"
        return queries

    def __init__(self):
        logging.basicConfig(level= logging.DEBUG)
        self.logger = logging.getLogger(__name__)


if __name__ == "__main__":
    logging.basicConfig(level= logging.DEBUG)
    logger = logging.getLogger(__name__)
    arguments = sys.argv[1:]
    num_dbs = int(arguments[0])
    max_tables = int(arguments[1])
    sensitive_generator = SensitiveTableGenerator()
    f = open("sensitiveCreationQueries.hql", "w")
    for i in range(num_dbs):
        logger.info("Creating queries for db number " + str(i))
        queries = sensitive_generator.create_db_and_table_queries(max_tables)
        f.write(queries + "\n")