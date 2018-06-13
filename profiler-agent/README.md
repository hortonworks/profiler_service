# Profiler Agent

Profiler Agent is central server for the Dataplane Profilers. Profiler Agent does the following :

- Registers Profilers 
- Launch profiler Jobs
- Monitor Profiler Jobs

Currently Profiler Agent uses Livy to launch profiler jobs.

### Terminologies

1. Profiler  —  Application that defines computation to be done. (contains jars and file locations etc)

2. Profiler Instance - Application instance with configurations. Used to run profiler job.

3. Profiler Job — The instance of a job for a profiler running on cluster. 

4. JobTask — Job task is the configurations that is used to launch profiler Job.

5. SelectorDefinition -  Selector will use asset source, apply filters and submit asset for profiling

   Currently  Agent only supports spark jobs which will be submitted to cluster using livy.

### Profilers

Profiler is a spark application that will run on cluster and does the  needed computation. Profilers are plugins to profiler agent that can be registered via REST call. Profiler Agent then can launch/monitor instance of profiler job (Spark App).

A profiler comprises of 

1. Jars — application jar + all dependent jars
2. Meta file — Description of profiler 

Directory Structure

```
lib 
  	abc.jar
  	def.jar
manifest
    meta.json
    *-profiler.conf
    *-selector.json
```

#### Content
###### meta.json
 ```
   {
     "name": "hive_column_profiler",
     "description": "Hive Column Profiler",
     "version": "${version}",
     "jobType": "Livy",
     "assetType": "Hive",
     "profilerConf": {
       "proxyUser": "{{PROFILER_USER}}",
       "file": "{{PROFILER_DIR}}/com.hortonworks.dataplane.hive-profiler-${version}.jar",
       "className": "ProfileTable",
       "jars": [
         "{{PROFILER_DIR}}/org.json4s.json4s-native_2.11-3.2.11.jar",
         "{{PROFILER_DIR}}/org.apache.atlas.atlas-common-${atlasVersion}.jar",
         "{{PROFILER_DIR}}/org.apache.atlas.atlas-typesystem-${atlasVersion}.jar",
         "{{PROFILER_DIR}}/org.apache.atlas.atlas-intg-${atlasVersion}.jar",
         "{{PROFILER_DIR}}/org.scalaj.scalaj-http_2.11-1.1.4.jar",
         "{{PROFILER_DIR}}/ch.qos.logback.logback-classic-1.1.2.jar",
         "{{PROFILER_DIR}}/com.typesafe.scala-logging.scala-logging_2.11-3.1.0.jar",
         "{{PROFILER_DIR}}/com.hortonworks.dataplane.commons-${version}.jar"
       ]
     },
     "user": "{{PROFILER_USER}}"
   }
   ```

###### <name>-profiler.conf
 ```
   {
     "name": "hivecolumn",
     "displayName": "Hive Column Profiler",
     "description": "Hive Column Profiler",
     "version": 1,
     "profilerId": 1,
     "profilerConf": {
     },
     "jobConf": {
       "samplepercent": "100"
     }
   }
   ```
   
###### <name>-selector.conf
 ```
   {
     "name": "hive_column-selector",
     "profilerInstanceName": "hivecolumn",
     "selectorType": "cron",
     "config": {"cronExpr": "0 55 * * * ?"},
     "sourceDefinition": {
       "sourceType": "hive-metastore",
       "config": {}
     },
     "pickFilters": [{
       "name": "inac",
       "config": {}
     }],
     "dropFilters": [{
       "name": "last-run",
       "config": {}
     }]
   }
   ```

While starting, Profiler agent will perform the following task for autoregistering profilers
1. Read `meta.json` and all `manifest/*-profiler.conf`
2. If profiler is not registered, upload `lib` directory to hdfs
3. Configure the {{PROFILER_DIR}} for profiler and register it to profiler agent.
4. Read all `manifest/*-selector.conf` and register all selectors


### Selector Definition
 Selector will have 
   1. Type -- define the selctor type

            a. cron  
   2. Source Definition  -- 
            
       a. Hive Metastore
       
       ```
             {
                "sourceType": "hive-metastore",
                "config": {}
             }
        ```
       
       b. Queue Source 
       
       size : Size of the queue
       
       assets  :  asset list to process
       
       ```
             {
             	"sourceType": "queue",
             	"config": {
             		"size": 10,
             		"assets": [{
             			"id": "dummy",
             			"assetType": "Hive",
             			"data": {}
             		}]
             	}
             }
       ```
        
   3. Filters (pick and drop) : 
        
        Filter Evaluation order : 
            1. If any drop filter evaluates true, asset is dropped
            2. If any pick filter evaluates true, asset is picked
            3. If no filter evaluates true, asset is dropped 
        
        a. All -- Pick all assets
        
        ```
            {
                "name": "all",
                "config": {}
            }

         ```
         
          Generally used as pick filter
        
        b. Last run --  True if asset runs successfully within minTimeSec time
        
        ```
           {
           	"name": "last-run",
           	"config": {
           		"minTimeSec": 172800
           	}
           }
        
        ```
        
            Generally used as drop filter
         
        c. In Asset Collection -- True if asset belong to some asset collection
        
            Generally used as pick filter
        
        ```
           {
           	"name": "inac",
           	"config": {}
           }
                
        ```


### Rest Api's

1. ##### Register Profiler

   **POST**  `/profilers`

   **Data** 

   ```
     {
       "name": "hivesensitivity",
       "version": "1.0.0",
       "jobType": "Livy",
       "assetType": "Hive",
       "profilerConf": {
         "jars": [
           "/apps/dpprofiler/profilers/sensitive_info_profiler/1.0.0.1.1.0.0-37/lib/ch.qos.logback.logback-classic-1.1.2.jar",
           "/apps/dpprofiler/profilers/sensitive_info_profiler/1.0.0.1.1.0.0-37/lib/com.typesafe.scala-logging.scala-logging_2.11-3.1.0.jar",
           "/apps/dpprofiler/profilers/sensitive_info_profiler/1.0.0.1.1.0.0-37/lib/org.scalaj.scalaj-http_2.11-1.1.4.jar"
         ],
         "proxyUser": "dpprofiler",
         "files": [
           "/apps/dpprofiler/profilers/sensitive_info_profiler/1.0.0.1.1.0.0-37/lib/keywords.json",
           "/apps/dpprofiler/profilers/sensitive_info_profiler/1.0.0.1.1.0.0-37/lib/labelstore.json"
         ],
         "className": "com.hortonworks.dataplane.profilers.SensitiveProfilerApp",
         "file": "/apps/dpprofiler/profilers/sensitive_info_profiler/1.0.0.1.1.0.0-37/lib/com.hortonworks.dataplane.sensitive-info-profiler-1.0.0.1.1.0.0-37.jar"
       },
       "user": "dpprofiler",
       "description": "Hive Sensitivity",
     }
   ```
   **Response**
   
   ```
      {
         "id": 1,
         "name": "hivesensitivity",
         "version": "1.0.0",
         "jobType": "Livy",
         "assetType": "Hive",
         "profilerConf": {
           "jars": [
             "/apps/dpprofiler/profilers/sensitive_info_profiler/1.0.0.1.1.0.0-37/lib/ch.qos.logback.logback-classic-1.1.2.jar",
             "/apps/dpprofiler/profilers/sensitive_info_profiler/1.0.0.1.1.0.0-37/lib/com.typesafe.scala-logging.scala-logging_2.11-3.1.0.jar",
             "/apps/dpprofiler/profilers/sensitive_info_profiler/1.0.0.1.1.0.0-37/lib/org.scalaj.scalaj-http_2.11-1.1.4.jar"
           ],
           "proxyUser": "dpprofiler",
           "files": [
             "/apps/dpprofiler/profilers/sensitive_info_profiler/1.0.0.1.1.0.0-37/lib/keywords.json",
             "/apps/dpprofiler/profilers/sensitive_info_profiler/1.0.0.1.1.0.0-37/lib/labelstore.json"
           ],
           "className": "com.hortonworks.dataplane.profilers.SensitiveProfilerApp",
           "file": "/apps/dpprofiler/profilers/sensitive_info_profiler/1.0.0.1.1.0.0-37/lib/com.hortonworks.dataplane.sensitive-info-profiler-1.0.0.1.1.0.0-37.jar"
         },
         "user": "dpprofiler",
         "description": "Hive Sensitivity",
         "created": 1523615709223
       }
      ```
   ​
2. ##### List Profilers
     **GET**  `/profilers`
     
     **Response**
     ```
          {
          	"id": 1,
          	"name": "hivesensitivity",
          	"version": "1.0.0",
          	"jobType": "Livy",
          	"assetType": "Hive",
          	"profilerConf": {
          		"jars": [
          			"/apps/dpprofiler/profilers/sensitive_info_profiler/1.0.0.1.1.0.0-37/lib/ch.qos.logback.logback-classic-1.1.2.jar",
          			"/apps/dpprofiler/profilers/sensitive_info_profiler/1.0.0.1.1.0.0-37/lib/com.typesafe.scala-logging.scala-logging_2.11-3.1.0.jar",
          			"/apps/dpprofiler/profilers/sensitive_info_profiler/1.0.0.1.1.0.0-37/lib/org.scalaj.scalaj-http_2.11-1.1.4.jar"
          		],
          		"proxyUser": "dpprofiler",
          		"files": [
          			"/apps/dpprofiler/profilers/sensitive_info_profiler/1.0.0.1.1.0.0-37/lib/keywords.json",
          			"/apps/dpprofiler/profilers/sensitive_info_profiler/1.0.0.1.1.0.0-37/lib/labelstore.json"
          		],
          		"className": "com.hortonworks.dataplane.profilers.SensitiveProfilerApp",
          		"file": "/apps/dpprofiler/profilers/sensitive_info_profiler/1.0.0.1.1.0.0-37/lib/com.hortonworks.dataplane.sensitive-info-profiler-1.0.0.1.1.0.0-37.jar"
          	},
          	"user": "dpprofiler",
          	"description": "Hive Sensitivity",
          	"created": 1523615709223
          }
     ```
           
3. ##### Update Profilers
     Update profiler by name and version
     
     **PUT**  `/profilers`
     
     **Data** 
   
      ```
        {
          "name": "hivesensitivity",
          "version": "1.0.0",
          "jobType": "Livy",
          "assetType": "Hive",
          "profilerConf": {
            "jars": [
              "/apps/dpprofiler/profilers/sensitive_info_profiler/1.0.0.1.1.0.0-37/lib/ch.qos.logback.logback-classic-1.1.2.jar",
              "/apps/dpprofiler/profilers/sensitive_info_profiler/1.0.0.1.1.0.0-37/lib/com.typesafe.scala-logging.scala-logging_2.11-3.1.0.jar",
              "/apps/dpprofiler/profilers/sensitive_info_profiler/1.0.0.1.1.0.0-37/lib/org.scalaj.scalaj-http_2.11-1.1.4.jar"
            ],
            "proxyUser": "dpprofiler",
            "files": [
              "/apps/dpprofiler/profilers/sensitive_info_profiler/1.0.0.1.1.0.0-37/lib/keywords.json",
              "/apps/dpprofiler/profilers/sensitive_info_profiler/1.0.0.1.1.0.0-37/lib/labelstore.json"
            ],
            "className": "com.hortonworks.dataplane.profilers.SensitiveProfilerApp",
            "file": "/apps/dpprofiler/profilers/sensitive_info_profiler/1.0.0.1.1.0.0-37/lib/com.hortonworks.dataplane.sensitive-info-profiler-1.0.0.1.1.0.0-37.jar"
          },
          "user": "dpprofiler",
          "description": "Hive Sensitivity",
        }
      ```
     
     **Response**
     ```
         {
         	"id": 1,
         	"name": "hivesensitivity",
         	"version": "1.0.0",
         	"jobType": "Livy",
         	"assetType": "Hive",
         	"profilerConf": {
         		"jars": [
         			"/apps/dpprofiler/profilers/sensitive_info_profiler/1.0.0.1.1.0.0-37/lib/ch.qos.logback.logback-classic-1.1.2.jar",
         			"/apps/dpprofiler/profilers/sensitive_info_profiler/1.0.0.1.1.0.0-37/lib/com.typesafe.scala-logging.scala-logging_2.11-3.1.0.jar",
         			"/apps/dpprofiler/profilers/sensitive_info_profiler/1.0.0.1.1.0.0-37/lib/org.scalaj.scalaj-http_2.11-1.1.4.jar"
         		],
         		"proxyUser": "dpprofiler",
         		"files": [
         			"/apps/dpprofiler/profilers/sensitive_info_profiler/1.0.0.1.1.0.0-37/lib/keywords.json",
         			"/apps/dpprofiler/profilers/sensitive_info_profiler/1.0.0.1.1.0.0-37/lib/labelstore.json"
         		],
         		"className": "com.hortonworks.dataplane.profilers.SensitiveProfilerApp",
         		"file": "/apps/dpprofiler/profilers/sensitive_info_profiler/1.0.0.1.1.0.0-37/lib/com.hortonworks.dataplane.sensitive-info-profiler-1.0.0.1.1.0.0-37.jar"
         	},
         	"user": "dpprofiler",
         	"description": "Hive Sensitivity",
         	"created": 1523615709223
         }
     ```

4. ##### Register Profiler Instance

   **POST**  `/profilerinstances`

   **Data** 

   ```
    {
          "name": "hivesensitivity",
          "displayName": "Hive Sensitivity",
          "profilerId": 1,
          "version": 1,
          "profilerConf": {
          	"executorCores": 4,
            "numExecutors": 8,
            "executorMemory" : "3g"
          },
          "jobConf": {
            "sampleSize": "100",
            "saveToAtlas": "true"
          },
          "active": true,
          "owner": "unknown",
          "queue": "default",
          "description": "Hive Sensitivity",
          "created": 1523615713368
        }
      }
   ```
   **Response**
   
   ```
     {
     	"id": 1,
     	"name": "hivesensitivity",
     	"displayName": "Hive Sensitivity",
     	"profilerId": 1,
     	"version": 1,
     	"profilerConf": {
     		"executorCores": 4,
     		"numExecutors": 8,
     		"executorMemory": "3g"
     	},
     	"jobConf": {
     		"sampleSize": "100",
     		"saveToAtlas": "true"
     	},
     	"active": true,
     	"owner": "unknown",
     	"queue": "default",
     	"description": "Hive Sensitivity"
     }
      ```
   ​
5. ##### List Profiler Instances
     **GET**  `/profilerinstances`
     
     **Response**
     ```
          [
            {
              "profiler": {
                ...
              },
              "profilerInstance": {
                "id": 1,
                "name": "hivesensitivity",
                "displayName": "Hive Sensitivity",
                "profilerId": 1,
                "version": 1,
                "profilerConf": {},
                "jobConf": {
                  "sampleSize": "100",
                  "saveToAtlas": "true"
                },
                "active": true,
                "owner": "unknown",
                "queue": "default",
                "description": "Hive Sensitivity",
                "created": 1523615713368
              }
            }
          ]
     ```
           
6. ##### Update Profiler Instance 
     Update profilerinstance by name. Version will autoincrement
     
     **PUT**  `/profilerinstances`
     
     **Data** 
   
      ```
       {
       	"name": "hivesensitivity",
       	"displayName": "Hive Sensitivity",
       	"profilerId": 1,
       	"version": 1,
       	"profilerConf": {
       		"executorCores": 2,
       		"numExecutors": 8,
       		"executorMemory": "3g"
       	},
       	"jobConf": {
       		"sampleSize": "100",
       		"saveToAtlas": "true"
       	},
       	"active": true,
       	"owner": "unknown",
       	"queue": "default",
       	"description": "Hive Sensitivity"
       }
      ```
     
     **Response**
     ```
         {
         	"id": 1,
         	"name": "hivesensitivity",
         	"displayName": "Hive Sensitivity",
         	"profilerId": 1,
         	"version": 1,
         	"profilerConf": {
         		"executorCores": 2,
         		"numExecutors": 8,
         		"executorMemory": "3g"
         	},
         	"jobConf": {
         		"sampleSize": "100",
         		"saveToAtlas": "true"
         	},
         	"active": true,
         	"owner": "unknown",
         	"queue": "default",
         	"description": "Hive Sensitivity",
         	"created": 1523615713368
         }
     ```

7. ##### Register Selector

   **POST**  `/profilerinstances`

   **Data** 

   ```
     {
     	"name": "hive-sensitivity-selector2",
     	"profilerInstanceName": "hivesensitivity",
     	"selectorType": "cron",
     	"config": {
     		"cronExpr": "0 0/3 * * * ?",
     		"scheduleOnStart": true
     	},
     	"sourceDefinition": {
     		"sourceType": "hive-metastore",
     		"config": {}
     	},
     	"pickFilters": [{
     		"name": "all",
     		"config": {}
     	}],
     	"dropFilters": [],
     }
   ```
   **Response**
   
   ```
     {
     	"id": 2,
     	"name": "hive-sensitivity-selector2",
     	"profilerInstanceName": "hivesensitivity",
     	"selectorType": "cron",
     	"config": {
     		"cronExpr": "0 0/3 * * * ?",
     		"scheduleOnStart": true
     	},
     	"sourceDefinition": {
     		"sourceType": "hive-metastore",
     		"config": {}
     	},
     	"pickFilters": [{
     		"name": "all",
     		"config": {}
     	}],
     	"dropFilters": [],
     	"lastUpdated": 1523615709171
     }
      ```

     
8. ##### List Selectors
     **GET**  `/selectors`
     
     **Response**
     ```
          [{
          	"id": 2,
          	"name": "hive-sensitivity-selector2",
          	"profilerInstanceName": "hivesensitivity",
          	"selectorType": "cron",
          	"config": {
          		"cronExpr": "0 0/3 * * * ?"
          	},
          	"sourceDefinition": {
          		"sourceType": "hive-metastore",
          		"config": {}
          	},
          	"pickFilters": [{
          		"name": "all",
          		"config": {}
          	}],
          	"dropFilters": [],
          	"lastUpdated": 1523615709171
          }]
     ```
           
9. ##### UpdateSelector
     Update selector by name
     
     **PUT**  `/selectors`
     
     **Data** 
   
      ```
       {
       	"id": 2,
       	"name": "hive-sensitivity-selector2",
       	"profilerInstanceName": "hivesensitivity",
       	"selectorType": "cron",
       	"config": {
       		"cronExpr": "0 0/3 * * * ?",
       		"scheduleOnStart" : true
       	},
       	"sourceDefinition": {
       		"sourceType": "hive-metastore",
       		"config": {}
       	},
       	"pickFilters": [{
       		"name": "all",
       		"config": {}
       	}],
       	"dropFilters": [],
       }
      ```
     
     **Response**
     ```
       {
       	"id": 2,
       	"name": "hive-sensitivity-selector2",
       	"profilerInstanceName": "hivesensitivity",
       	"selectorType": "cron",
       	"config": {
       		"cronExpr": "0 0/3 * * * ?",
       		"scheduleOnStart": true
       	},
       	"sourceDefinition": {
       		"sourceType": "hive-metastore",
       		"config": {}
       	},
       	"pickFilters": [{
       		"name": "all",
       		"config": {}
       	}],
       	"dropFilters": [],
       	"lastUpdated": 1523615709171
       }
     ```
     
10. ##### Update Selector Config
     Update selector Config
     
     **PUT**  `/selectors/:name/config`
     
     **Data** 
   
      ```
    {
    	"cronExpr": "0 0/6 * * * ?",
    	"scheduleOnStart": true
    }
      ```
     
     **Response**
     ```
       {
       	"id": 2,
       	"name": "hive-sensitivity-selector2",
       	"profilerInstanceName": "hivesensitivity",
       	"selectorType": "cron",
       	"config": {
       		"cronExpr": "0 0/6 * * * ?",
       		"scheduleOnStart": true
       	},
       	"sourceDefinition": {
       		"sourceType": "hive-metastore",
       		"config": {}
       	},
       	"pickFilters": [{
       		"name": "all",
       		"config": {}
       	}],
       	"dropFilters": [],
       	"lastUpdated": 1523615709171
       }
     ```

11. ##### Launch Profiler Job
      **POST**  `/jobs`
      
      **DATA**
      
      ```
           {
              "profilerInstanceName" : "hivecolumn",
              "assets" :[{
                "id":"default.sampleTable",
                "assetType" : "Hive",
                "data" : {
                    "db" : "default",
                    "table" : "sampleTable"
                }
              }]
           }
      ```     
           
      **RESPONSE**
      
      ```
           {
                "id": 4,
                "profilerId": 2,
                "status": "STARTED",
                "jobType": "Livy",
                "conf": {},
                "details": {
                  "sparkUiUrl": "",
                  "state": "starting",
                  "id": "13",
                  "appId": "",
                  "driverLogUrl": "",
                  "log": ""
                },
                "description": "Livy Job",
                "jobUser": "hdfs",
                "submitter": "unknown",
                "start": 1503656104864,
                "lastUpdated": 1503656104865
           }
      ```                

12. ###### Get Job status
      **GET**  `/jobs/:id`
      
      **RESPONSE**
      
      ```
          {
            "id": 4,
            "profilerId": 2,
            "status": "RUNNING",
            "jobType": "Livy",
            "conf": {},
            "details": {
              "sparkUiUrl": "http://ctr-e134-1499953498516-108742-01-000002.hwx.site:8088/proxy/application_1503650077177_0003/",
              "state": "starting",
              "id": "13",
              "appId": "application_1503650077177_0003",
              "driverLogUrl": "",
              "log": "\t ApplicationMaster RPC port: -1\n\t queue: default\n\t start time: 1503656109414\n\t final status: UNDEFINED\n\t tracking URL: http://ctr-e134-1499953498516-108742-01-000002.hwx.site:8088/proxy/application_1503650077177_0003/\n\t user: hdfs\n17/08/25 10:15:09 INFO ShutdownHookManager: Shutdown hook called\n17/08/25 10:15:09 INFO ShutdownHookManager: Deleting directory /tmp/spark-4f99f957-391f-465d-9166-b602e46feb40\nYARN Diagnostics:\n[Fri Aug 25 10:15:09 +0000 2017] Application is added to the scheduler and is not yet activated. Queue's AM resource limit exceeded.  Details : AM Partition = <DEFAULT_PARTITION>; AM Resource Request = <memory:2048, vCores:1>; Queue Resource Limit for AM = <memory:5120, vCores:1>; User AM Resource Limit of the queue = <memory:5120, vCores:1>; Queue AM Resource Usage = <memory:4096, vCores:1>; "
            },
            "description": "Livy Job",
            "jobUser": "hdfs",
            "submitter": "unknown",
            "start": 1503656104864,
            "lastUpdated": 1503656104865
          }
      ```  

13. ###### Kill job
      **DELETE**  `/jobs/:id`
      
      **RESPONSE**
            
           {
             "id": 4,
             "profilerId": 2,
             "status": "RUNNING",
             "jobType": "Livy",
             "conf": {},
             "details": {
               "sparkUiUrl": "http://ctr-e134-1499953498516-108742-01-000002.hwx.site:8088/proxy/application_1503650077177_0003/",
               "state": "starting",
               "id": "13",
               "appId": "application_1503650077177_0003",
               "driverLogUrl": "",
               "log": "\t ApplicationMaster RPC port: -1\n\t queue: default\n\t start time: 1503656109414\n\t final status: UNDEFINED\n\t tracking URL: http://ctr-e134-1499953498516-108742-01-000002.hwx.site:8088/proxy/application_1503650077177_0003/\n\t user: hdfs\n17/08/25 10:15:09 INFO ShutdownHookManager: Shutdown hook called\n17/08/25 10:15:09 INFO ShutdownHookManager: Deleting directory /tmp/spark-4f99f957-391f-465d-9166-b602e46feb40\nYARN Diagnostics:\n[Fri Aug 25 10:15:09 +0000 2017] Application is added to the scheduler and is not yet activated. Queue's AM resource limit exceeded.  Details : AM Partition = <DEFAULT_PARTITION>; AM Resource Request = <memory:2048, vCores:1>; Queue Resource Limit for AM = <memory:5120, vCores:1>; User AM Resource Limit of the queue = <memory:5120, vCores:1>; Queue AM Resource Usage = <memory:4096, vCores:1>; "
             },
             "description": "Livy Job",
             "jobUser": "hdfs",
             "submitter": "unknown",
             "start": 1503656104864,
             "lastUpdated": 1503656104865
           }
           
           
14. ##### Metrics Api
      **POST**  `/assetmetrics`
      
      **DATA**
      
      ```
          {
          	"metrics": [{
          		"metric": "hiveagg", 
          		"aggType": "Daily"
          	}],
          	"sql": "SELECT date, data.`action` from hiveagg_daily where database='abc' and table='t1' order by date asc"
          }
      ```     
           
      **RESPONSE**
      
      ```
           {
             "data": [
               {
                 "date": "2018-03-28",
                 "action": {
                   "drop": 16,
                   "select": 58,
                   "create": 3
                 }
               },
               {
                 "date": "2018-03-29",
                 "action": {
                   "drop": 27,
                   "select": 69
                 }
               },
               {
                 "date": "2018-03-30",
                 "action": {
                   "drop": 29,
                   "select": 67
                 }
               },
               {
                 "date": "2018-03-31",
                 "action": {
                   "drop": 16,
                   "select": 80
                 }
               }
             ]
           }
      ```             