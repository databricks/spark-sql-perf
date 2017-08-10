# Spark SQL Performance Tests

[![Build Status](https://travis-ci.org/databricks/spark-sql-perf.svg)](https://travis-ci.org/databricks/spark-sql-perf)

This is a performance testing framework for [Spark SQL](https://spark.apache.org/sql/) in [Apache Spark](https://spark.apache.org/) 1.6+.

**Note: This README is still under development. Please also check our source code for more information.**

# Quick Start

```
$ bin/run --help

spark-sql-perf 0.2.0
Usage: spark-sql-perf [options]

  -b <value> | --benchmark <value>
        the name of the benchmark to run
  -f <value> | --filter <value>
        a filter on the name of the queries to run
  -i <value> | --iterations <value>
        the number of iterations to run
  --help
        prints this usage text
        
$ bin/run --benchmark DatasetPerformance
```

### MLlib tests

To run MLlib tests, run `/bin/run-ml yamlfile`, where `yamlfile` is the path to a YAML configuration
file describing tests to run and their parameters.

# TPC-DS

## How to use it
The rest of document will use TPC-DS benchmark as an example. We will add contents to explain how to use other benchmarks add the support of a new benchmark dataset in future.

### Setup a benchmark
Before running any query, a dataset needs to be setup by creating a `Benchmark` object. Generating
the TPCDS data requires dsdgen built and available on the machines. We have a fork of dsdgen that
you will need. It can be found [here](https://github.com/davies/tpcds-kit).  

```
// If not done already, you have to set the path for the results
spark.config("spark.sql.perf.results", "/tmp/results")

import com.databricks.spark.sql.perf.tpcds.Tables
// Tables in TPC-DS benchmark used by experiments.
// dsdgenDir is the location of dsdgen tool installed in your machines.
// scaleFactor defines the size of the dataset to generate (in GB)
val tables = new Tables(sqlContext, dsdgenDir, scaleFactor)

// Generate data.
// location is the place there the generated data will be written
// format is a valid spark format like "parquet"
tables.genData(location, format, overwrite, partitionTables, useDoubleForDecimal, clusterByPartitionColumns, filterOutNullPartitionValues)
// Create metastore tables in a specified database for your data.
// Once tables are created, the current database will be switched to the specified database.
tables.createExternalTables(location, format, databaseName, overwrite)
// Or, if you want to create temporary tables
tables.createTemporaryTables(location, format)
// Setup TPC-DS experiment
import com.databricks.spark.sql.perf.tpcds.TPCDS
val tpcds = new TPCDS (sqlContext = sqlContext)
```

### Run benchmarking queries
After setup, users can use `runExperiment` function to run benchmarking queries and record query execution time. Taking TPC-DS as an example, you can start an experiment by using

```
val experiment = tpcds.runExperiment(tpcds.interactiveQueries)
experiment.waitForFinish(60*60*10) // optional: wait for results (with timeout)
```

For every experiment run (i.e. every call of `runExperiment`), Spark SQL Perf will use the timestamp of the start time to identify this experiment. Performance results will be stored in the sub-dir named by the timestamp in the given `spark.sql.perf.results` (for example `/tmp/results/timestamp=1429213883272`). The performance results are stored in the JSON format.

### Retrieve results
While the experiment is running you can use `experiment.html` to list the status.  Once the experiment is complete, you can load the results from disk.

```
// Get all experiments results.
val resultTable = spark.read.json(spark.conf.get("spark.sql.perf.results"))
resultTable.createOrReplaceTempView("sqlPerformance")
sqlContext.table("sqlPerformance")
// Get the result of a particular run by specifying the timestamp of that run.
sqlContext.table("sqlPerformance").filter("timestamp = 1429132621024")
// or
val specificResultTable = spark.read.json(experiment.resultPath)
```
