#!/usr/bin/env bash

"""
Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.

Except as expressly permitted in a written agreement between you or your company
and Hortonworks, Inc. or an authorized affiliate or partner thereof, any use,
reproduction, modification, redistribution, sharing, lending or other exploitation
of all or any part of the contents of this software is strictly prohibited.
"""

set -x
size=$1
unzip hive-testbench.zip
cd hive-testbench

echo "Creating home directory for root on HDFS ... "
sudo -u hdfs hadoop fs -mkdir /user/root
sudo -u hdfs hadoop fs -chown root /user/root

echo "Setting up requirements ... "
./tpcds-build.sh

echo "Populating TPC data ... "
./tpcds-setup.sh $size