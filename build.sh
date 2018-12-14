#!/usr/bin/env bash

source ./profiler_agent_build.properties

rm -rf ./artifacts

mkdir -p ./artifacts/hdp2

mkdir -p ./artifacts/hdp3

./build/sbt -mem 2048 -Dhdp.interface.version=2.6-0.1 -Ddss.version=$dss_version -Dspark.version=$hdp2_spark_version -Datlas.version=$hdp2_atlas_version -Dhive.metastore.version=$hdp2_hive_metastore_version -Dhadoop.version=$hdp2_hadoop_version "$@" clean dist

cp ./profiler-agent/target/universal/*.zip ./artifacts/hdp2
cp ./profilers/**/target/universal/*.zip ./artifacts/hdp2

./build/sbt -mem 2048 -Dhdp.interface.version=3.0-0.1 -Ddss.version=$dss_version -Dspark.version=$hdp3_spark_version -Datlas.version=$hdp2_atlas_version -Dhive.metastore.version=$hdp3_hive_metastore_version -Dhadoop.version=$hdp3_hadoop_version "$@" clean dist

cp ./profiler-agent/target/universal/*.zip ./artifacts/hdp3
cp ./profilers/**/target/universal/*.zip ./artifacts/hdp3
