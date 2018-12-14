#!/usr/bin/env bash
"""
Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.

Except as expressly permitted in a written agreement between you or your company
and Hortonworks, Inc. or an authorized affiliate or partner thereof, any use,
reproduction, modification, redistribution, sharing, lending or other exploitation
of all or any part of the contents of this software is strictly prohibited.
"""

set -x
db=$1
table=$2

/usr/hdp/current/spark2-client/bin/spark-submit --master yarn --deploy-mode cluster --queue poor_queue --jars /root/data/json4s-native_2.11-3.2.10.jar,/root/data/atlas-typesystem-0.8-incubating.jar,/root/data/atlas-common-0.8-incubating.jar,/root/data/scalaj-http_2.11-1.1.4.jar,/root/data/logback-classic-1.1.2.jar,/root/data/scala-logging_2.11-3.1.0.jar --class ProfileTable /root/data/hive-profiler_2.11-1.0.0-SNAPSHOT.jar "{\"context\":{\"profilername\":\"hivecolumn\",\"profilerversion\":\"1.0.0\"},\"clusterconfigs\":{\"clusterName\":\"profiler_perf\",\"metastoreUrl\":\"thrift://profiler-perf-2.openstacklocal:9083\",\"atlasUrl\":\"http://profiler-perf-1.openstacklocal:21000\",\"atlasUser\":\"admin\",\"atlasPassword\":\"admin\"},\"profilerconf\":{\"samplepercent\":\"100\"},\"assets\":[{\"db\":\""$db"\",\"table\":\""$table"\"}]}"