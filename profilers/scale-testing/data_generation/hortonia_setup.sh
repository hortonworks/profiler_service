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
echo "Got HiveServer URL => " $host
jdbc_url="jdbc:hive2://"$host":10000/default"
echo "Constructed JDBC URL => " $jdbc_url

echo "Unpacking TARball ..."
tar -xzvf sko_demo_data.tar.gz

cp -R sko_demo_data /tmp/
chmod -R 777 /tmp/sko_demo_data

cd /tmp/sko_demo_data

echo "Uploading data files to HDFS ..."

su hdfs -c "hdfs dfs -mkdir /user/insurance_admin && hdfs dfs -chown insurance_admin /user/insurance_admin"
su hdfs -c "hdfs dfs -mkdir /user/marketing_admin && hdfs dfs -chown marketing_admin /user/marketing_admin"
su hdfs -c "hdfs dfs -mkdir /sko_demo_data"

su hdfs -c "hdfs dfs -put /tmp/sko_demo_data/* /sko_demo_data/"
su hdfs -c "hdfs dfs -chown -R marketing_admin /sko_demo_data/structured/marketing_campaigns"
su hdfs -c "hdfs dfs -chown -R insurance_admin /sko_demo_data/structured/hortonia_bank"
su hdfs -c "hdfs dfs -chown -R insurance_admin /sko_demo_data/unstructured/*"
su hdfs -c "hdfs dfs -chmod -R 777 /sko_demo_data/"

echo "Data upload and permission setting done"

echo "Exceuting beeline command to create Hive tables"

beeline -u  $jdbc_url -n insurance_admin -p admin -f scripts/hortonia_create_ddl.sql 