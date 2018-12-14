# Profiler Configurations

The profiler configurtions will have to be set at a per cluster level. The following are the configuration options:

Parameters | Description
-----------|---------------
file | File containing the application to execute | path (required)
proxyUser | User to impersonate when running the job | string
className | Application Java/Spark main class | string
args | Command line arguments for the application | list of strings
jars | jars to be used in this session | List of string
pyFiles | Python files to be used in this session | List of string
files | files to be used in this session | List of string
driverMemory | Amount of memory to use for the driver process | string
driverCores | Number of cores to use for the driver process | int
executorMemory | Amount of memory to use per executor process | string
executorCores | Number of cores to use for each executor | int
numExecutors | Number of executors to launch for this session | int
archives | Archives to be used in this session | List of string
queue | The name of the YARN queue to which submitted | string
name | The name of this session | string



# Sample Profiler Instance Configuration

```sh

{
    "name": "sensitiveinfo",
    "displayName": "Sensitive Profiler",
    "profilerId": 3,
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
    "description": "Sensitive Profiler",
    "created": 1525339140000
  }

```


   
