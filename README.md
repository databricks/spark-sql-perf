# Spark SQL Performance Tests

This is a performance testing framework for [Spark SQL](https://spark.apache.org/sql/) in [Apache Spark](https://spark.apache.org/) 1.4+.

**Note: This README is still under development. Please also check our source code for more information.**

## How to use it
The rest of document will use TPC-DS benchmark as an example. We will add contents to explain how to use other benchmarks add the support of a new benchmark dataset in future.

### Setup a benchmark
Before running any query, a dataset needs to be setup by creating a `Benchmark` object.   

```
import org.apache.spark.sql.parquet.Tables
// Tables in TPC-DS benchmark used by experiments.
val tables = Tables(sqlContext)
// Setup TPC-DS experiment
val tpcds = new TPCDS (sqlContext = sqlContext)
```

### Run benchmarking queries
After setup, users can use `runExperiment` function to run benchmarking queries and record query execution time. Taking TPC-DS as an example, you can start an experiment by using

```
val experiment = tpcds.runExperiment(queriesToRun = tpcds.interactiveQueries)
```

For every experiment run (i.e.\ every call of `runExperiment`), Spark SQL Perf will use the timestamp of the start time to identify this experiment. Performance results will be stored in the sub-dir named by the timestamp in the given `resultsLocation` (for example `results/1429213883272`). The performance results are stored in the JSON format.

### Retrieve results
While the experiment is running you can use `experiment.html` to list the status.  Once the experiment is complete, the results will be saved to the table sqlPerformance in json.

```
// Get experiments results.
import com.databricks.spark.sql.perf.Results
val results = Results(resultsLocation = <the root location of performance results>, sqlContext = sqlContext)
// Get the DataFrame representing all results stored in the dir specified by resultsLocation.
val allResults = results.allResults
// Use DataFrame API to get results of a single run.
allResults.filter("timestamp = 1429132621024")
```