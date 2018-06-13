Below is the sample command to run the Geo Location Indentifier Profiler

/usr/hdp/current/spark2-client/bin/spark-submit --proxy-user hdfs --files 
/usr/hdp/current/spark2-client/conf/hive-site.xml,/root/data/en-token.bin,
/root/data/en-ner-location.bin --master yarn --deploy-mode cluster --jars 
/root/data/json4s-native_2.11-3.2.10.jar,/root/data/opennlp-tools-1.8.0.jar,
/root/data/atlas-typesystem-0.8-incubating.jar,/root/data/atlas-common-0.8-incubating.jar,
/root/data/scalaj-http_2.11-1.1.4.jar,/root/data/stringmetric-core_2.11-0.27.4.jar,
/root/data/atlas-commons_2.11-1.0.jar  --class GeoLocationProfiler 
/root/data/geo-profiler_2.11-1.0.jar hortoniabank us_customers Geo_Profiler http://172.22.114.47:21000


hortoniabank - Hive Database
us_customers - Hive Table
Geo_Profiler - Cluster Name
http://172.22.114.47:21000 - Atlas URL