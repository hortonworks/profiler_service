#!/usr/bin/env bash
"""
Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.

Except as expressly permitted in a written agreement between you or your company
and Hortonworks, Inc. or an authorized affiliate or partner thereof, any use,
reproduction, modification, redistribution, sharing, lending or other exploitation
of all or any part of the contents of this software is strictly prohibited.
"""
set -x
host=$1
num_dbs=$2
num_tables=$3
python sensitive_table_generator.py $num_dbs $num_tables

echo "Got HiveServer URL => " $host
jdbc_url="jdbc:hive2://"$host":10000/default"
echo "Constructed JDBC URL => " $jdbc_url

echo "Creating tables in Hive ... "
cp sensitiveCreationQueries.hql /tmp/
cd /tmp
chmod 777 sensitiveCreationQueries.hql

su hdfs -c "beeline -u  $jdbc_url -n hdfs -p hdfs -f sensitiveCreationQueries.hql"