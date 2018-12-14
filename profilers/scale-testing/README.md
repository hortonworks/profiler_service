This directory contains the scripts to generate and populate various datasets needed for 
Hive profiler testing

1) To generate TPC dataset, copy files tpc_setup.sh and hive-testbench.zip to cluster machine and execute:

    ./tpc_setup.sh $SIZE_IN_GB

    Example -> ./tpc_setup.sh 100 
    The above command will generate 100 GB of data in the underlying Hive instance on the cluster
    
    
2) To generate Hortonia dataset, copy hortonia_setup.sh and sko_demo_data.tar.gz to cluster
   machine and execute:
   
   ./hortonia_setup $HIVESERVER2_URL
   
   where HIVESERVER2_URL is the machine qualified name or IP address on which HiveServer2 is 
   installed.
   
   Example -> ./hortonia_setup ctr-e134-1499953498516-107783-01-000002.hwx.site
   
   
3) To create an arbitrary number of tables in Hive instance on a cluster, execute:
   
   ./create_hive_tables.sh $NUMBER_OF_TABLES
   
   For example ./create_hive_tables.sh 100
   
   The above command will create 100 Hive tables with arbitrary number of columns with 
   arbitrary data types. Note that the create tables will be empty.
   

