# Spark SQL Performance Tests

[![Build Status](https://travis-ci.org/databricks/spark-sql-perf.svg)](https://travis-ci.org/databricks/spark-sql-perf)

This is a performance testing framework for [Spark SQL](https://spark.apache.org/sql/) in [Apache Spark](https://spark.apache.org/) 2.2+.

**Note: This README is still under development. Please also check our source code for more information.**

# Quick Start

## Running from command line.

```
$ bin/run --help

spark-sql-perf 0.2.0
Usage: spark-sql-perf [options]

  -b <value> | --benchmark <value>
        the name of the benchmark to run
  -m <value> | --master <value
        the master url to use
  -f <value> | --filter <value>
        a filter on the name of the queries to run
  -i <value> | --iterations <value>
        the number of iterations to run
  --help
        prints this usage text
        
$ bin/run --benchmark DatasetPerformance
```

The first run of `bin/run` will build the library.

## Build

Use `sbt package` or `sbt assembly` to build the library jar.  
Use `sbt +package` to build for scala 2.11 and 2.12.

## Local performance tests
The framework contains twelve benchmarks that can be executed in local mode. They are organized into three classes and target different components and functions of Spark:
* [DatasetPerformance](https://github.com/databricks/spark-sql-perf/blob/master/src/main/scala/com/databricks/spark/sql/perf/DatasetPerformance.scala) compares the performance of the old RDD API with the new Dataframe and Dataset APIs.
These benchmarks can be launched with the command `bin/run --benchmark DatasetPerformance`
* [JoinPerformance](https://github.com/databricks/spark-sql-perf/blob/master/src/main/scala/com/databricks/spark/sql/perf/JoinPerformance.scala) compares the performance of joining different table sizes and shapes with different join types.
These benchmarks can be launched with the command `bin/run --benchmark JoinPerformance`
* [AggregationPerformance](https://github.com/databricks/spark-sql-perf/blob/master/src/main/scala/com/databricks/spark/sql/perf/AggregationPerformance.scala) compares the performance of aggregating different table sizes using different aggregation types.
These benchmarks can be launched with the command `bin/run --benchmark AggregationPerformance`


# MLlib tests

To run MLlib tests, run `/bin/run-ml yamlfile`, where `yamlfile` is the path to a YAML configuration
file describing tests to run and their parameters.

# TPC-DS

## Setup a benchmark

Before running any query, a dataset needs to be setup by creating a `Benchmark` object. Generating
the TPCDS data requires dsdgen built and available on the machines. We have a fork of dsdgen that
you will need. The fork includes changes to generate TPCDS data to stdout, so that this library can
pipe them directly to Spark, without intermediate files. Therefore, this library will not work with
the vanilla TPCDS kit.

TPCDS kit needs to be installed on all cluster executor nodes under the same path!

It can be found [here](https://github.com/databricks/tpcds-kit).  

```
// Generate the data
build/sbt "test:runMain com.databricks.spark.sql.perf.tpcds.GenTPCDSData -d <dsdgenDir> -s <scaleFactor> -l <location> -f <format>"
```

```
// Create the specified database
sql(s"create database $databaseName")
// Create metastore tables in a specified database for your data.
// Once tables are created, the current database will be switched to the specified database.
tables.createExternalTables(rootDir, "parquet", databaseName, overwrite = true, discoverPartitions = true)
// Or, if you want to create temporary tables
// tables.createTemporaryTables(location, format)

// For CBO only, gather statistics on all columns:
tables.analyzeTables(databaseName, analyzeColumns = true) 
```

## Run benchmarking queries
After setup, users can use `runExperiment` function to run benchmarking queries and record query execution time. Taking TPC-DS as an example, you can start an experiment by using

```
import com.databricks.spark.sql.perf.tpcds.TPCDS

val tpcds = new TPCDS (sqlContext = sqlContext)
// Set:
val databaseName = ... // name of database with TPCDS data.
val resultLocation = ... // place to write results
val iterations = 1 // how many iterations of queries to run.
val queries = tpcds.tpcds2_4Queries // queries to run.
val timeout = 24*60*60 // timeout, in seconds.
// Run:
sql(s"use $databaseName")
val experiment = tpcds.runExperiment(
  queries, 
  iterations = iterations,
  resultLocation = resultLocation,
  forkThread = true)
experiment.waitForFinish(timeout)
```

By default, experiment will be started in a background thread.
For every experiment run (i.e. every call of `runExperiment`), Spark SQL Perf will use the timestamp of the start time to identify this experiment. Performance results will be stored in the sub-dir named by the timestamp in the given `spark.sql.perf.results` (for example `/tmp/results/timestamp=1429213883272`). The performance results are stored in the JSON format.

## Retrieve results
While the experiment is running you can use `experiment.html` to get a summary, or `experiment.getCurrentResults` to get complete current results.
Once the experiment is complete, you can still access `experiment.getCurrentResults`, or you can load the results from disk.

```
// Get all experiments results.
val resultTable = spark.read.json(resultLocation)
resultTable.createOrReplaceTempView("sqlPerformance")
sqlContext.table("sqlPerformance")
// Get the result of a particular run by specifying the timestamp of that run.
sqlContext.table("sqlPerformance").filter("timestamp = 1429132621024")
// or
val specificResultTable = spark.read.json(experiment.resultPath)
```

You can get a basic summary by running:
```
experiment.getCurrentResults // or: spark.read.json(resultLocation).filter("timestamp = 1429132621024")
  .withColumn("Name", substring(col("name"), 2, 100))
  .withColumn("Runtime", (col("parsingTime") + col("analysisTime") + col("optimizationTime") + col("planningTime") + col("executionTime")) / 1000.0)
  .select('Name, 'Runtime)
```

# TPC-H

TPC-H can be run similarly to TPC-DS replacing `tpcds` for `tpch`.
Take a look at the data generator and `tpch_run` notebook code below.

## Running in Databricks workspace (or spark-shell)

There are example notebooks in `src/main/notebooks` for running TPCDS and TPCH in the Databricks environment.
_These scripts can also be run from spark-shell command line with minor modifications using `:load file_name.scala`._

### TPC-multi_datagen notebook
This notebook (or scala script) can be use to generate both TPCDS and TPCH data at selected scale factors.
It is a newer version from the `tpcds_datagen` notebook below.  To use it:
* Edit the config variables the top of the script.
* Run the whole notebook.

### tpcds_datagen notebook

This notebook can be used to install dsdgen on all worker nodes, run data generation, and create the TPCDS database.
Note that because of the way dsdgen is installed, it will not work on an autoscaling cluster, and `num_workers` has
to be updated to the number of worker instances on the cluster.
Data generation may also break if any of the workers is killed - the restarted worker container will not have `dsdgen` anymore.
 
### tpcds_run notebook

This notebook can be used to run TPCDS queries.

For running parallel TPCDS streams:
* Create a Cluster and attach the spark-sql-perf library to it.
* Create a Job using the notebook and attaching to the created cluster as "existing cluster".
* Allow concurrent runs of the created job.
* Launch appriopriate number of Runs of the Job to run in parallel on the cluster.

### tpch_run notebook

This notebook can be used to run TPCH queries.  Data needs be generated first.