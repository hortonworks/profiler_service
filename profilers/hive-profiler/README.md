# Hive Column Profiler

Below are the metrics which are computed for the corresponding column Types

Column Type |  Metrics
------------|--------------
Numeric | cardinality, mean, median, max, min, stddev, histogram, quartiles, frequent items |
String | cardinality, meanLength, medianLength, maxLength, minLength, stdLength, histogram, quartiles, frequent items |
Boolean | numTrues, numFalse, numNull |


These metrics are persisted to Atlas

Please note that the complex column types like Map, List, Struct are not supported by Hive Column Profiler