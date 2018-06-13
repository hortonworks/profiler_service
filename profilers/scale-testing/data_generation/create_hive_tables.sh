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
num_tables=$2
num_columns=$3
num_rows=$4
python generate_tables.py $num_tables $num_columns $num_rows

echo "Got HiveServer URL => " $host
jdbc_url="jdbc:hive2://"$host":10000/default"
echo "Constructed JDBC URL => " $jdbc_url

echo "Creating tables in Hive ... "
cp tableCreationQueries.hql /tmp/
cd /tmp
chmod 777 tableCreationQueries.hql

su hdfs -c "beeline -u  $jdbc_url -n hdfs -p hdfs -f tableCreationQueries.hql"