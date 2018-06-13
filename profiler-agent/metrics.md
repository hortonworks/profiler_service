# Profiler Metrics

Metrics are the output of profilers stored on HDFS as orc/parquet. 

These can be accessed using Profiler Agent Rest API with standard SQL interface. Profiler Agent uses Livy interactive to serve queries on the tables.



### Hive Metastore Profiler

Stores metadata for hive tables.

##### Schema

| Column Name   | Data Type |
| ------------- | --------- |
| table         | String    |
| db            | String    |
| createTime    | Date      |
| size          | Long      |
| numRows       | Long      |
| numPartitions | Long      |
| inputFormat   | String    |
| outputFormat  | String    |



##### Metric Name

```
hivemetastorestats
```

##### Aggregations

```
Snapshot
```

##### Sample Query

```
{
  "metric" : "hivemetastorestats",
  "aggType" : "Snapshot",
  "sql" : "select table, size from hivemetastorestats_snapshot limit 10"
}
```

##### Recommended Queries

| Queries                                  |
| ---------------------------------------- |
| select table, size from hivemetastorestats_snapshot order by size desc limit 10 |
| select table, numRows from hivemetastorestats_snapshot order by numRows desc limit 10 |
| select table, numPartitions from hivemetastorestats_snapshot order by numPartitions desc limit 10 |
| select db, count(*) as tableCount from hivemetastorestats_snapshot group by db order by tableCount desc limit 10 |
| select inputFormat, count(*) as tableCount from hivemetastorestats_snapshot group by inputFormat order by tableCount desc limit 10 |
| select outputFormat, count(*) as tableCount from hivemetastorestats_snapshot group by outputFormat order by tableCount desc limit 10 |
| select table, createTime from hivemetastorestats_snapshot order by createTime desc limit 10 |
| select db, max(size) as max_size, min(size) as min_size, max(numRows) as max_rows, min(numRows) as min_rows,max(numPartitions) as max_partitions, min(numPartitions) as min_partitions, max(createTime) as max_create_time, min(createTime) as min_create_time from hivemetastorestats_snapshot group by db |



### Hive Sensitivity Profiler

Contains sensitive columns information

##### Schema

| Column Name   | Data Type |
| ------------- | --------- |
| table         | String    |
| database      | String    |
| column        | String    |
| label         | String    |
| percentMatch  | Double    |


##### Metric Name

```
hivesensitivity
```

##### Aggregations

```
Snapshot
```

##### Sample Query

```
{
  "metric" : "hivesensitivity",
  "aggType" : "Snapshot",
  "sql" : "select * from hivesensitivity_snapshot limit 10"
}
```

##### Recommended Queries

| Queries                                  |
| ---------------------------------------- |
| select * from hivesensitivity_snapshot limit 10 |