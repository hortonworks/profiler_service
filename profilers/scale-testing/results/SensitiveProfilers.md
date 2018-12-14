# Sensitivity Profilers Scale Testing Results
This document outlines a comparison of the execution times of the queries in the two candidate 
approaches for the storage of sensitive information data on HDFS. 
Below are the high level overview of the approaches:

### Approach 1
Dump the extracted sensitive information into a separate partition with structure db=dbName/table=tableName. 
Read from all such partitions under common HDFS directory(say /sensitiveInfo) and 
construct the dataframe for subsequent querying

### Approach 2
Compute the sensitive information for all the databases and tables and dump all the information 
into a single file in HDFS. Read from this file and convert into data frame for querying

### Estimated Scale
It is reasonable to assume 1 million entries in total. Below is the rationale for this estimate 
on a typical production database:

- Number of Databases - 50
- Number of tables per database - 200
- Number of rows per table - 100

An average table would have about 50 columns. Assuming generously that every column will match 
two labels, there will be 100 entries for a (db, table) pair

Below are the queries which were used for the benchmarking of the execution times:
- Query 1 - Top 10 databases with most number of sensitive labels
```
    select database, count(*) as count from sensitiveInfoPartitioned group by database order by count desc limit 10
```

- Query 2 - Top 10 (database, label) pairs
```
    select database, label, count(*) as count from sensitiveInfoPartitioned group by database, label order by count desc limit 10
```
- Query 3 - Top 10 tables with most number of sensitive labels
```
    select table, count(*) as count from sensitiveInfoPartitioned group by table order by count desc limit 10
```

- Query 4 - Top 10 (table, label) pairs
```
    select table, label, count(*) as count from sensitiveInfoPartitioned group by table, label order by count desc limit 10
```

Below are the execution time results for the two approaches. All times are in seconds

| Strategy | Query 1 | Query 2| Query 3 | Query 4 | Compute Overhead
|-----------| -------- |-------- | -------- | ------- | ----------- |
Partition Approach | 2.9 | 7.2 | 2.8 | 8.4 | NA|
Hybrid Approach | 0.4 | 0.7 | 0.4 | 0.6 | 73



Based on the above results, we decided to adopt the hybrid startegy to store the results of sensitivity profilers