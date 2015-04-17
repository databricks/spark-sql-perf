# Spark SQL Performance Tests

This is a performance testing framework for [Spark SQL](https://spark.apache.org/sql/) in [Apache Spark](https://spark.apache.org/) 1.3+.

**Note: This README is still under development. Please also check our source code for more information.**

## How to use it
The rest of document will use TPC-DS benchmark as an example. We will add contents to explain how to use other benchmarks add the support of a new benchmark dataset in future.

### Setup a dataset
Before running any query, a dataset needs to be setup by creating a `Dataset` object. Every benchmark support in Spark SQL Perf needs to implement its own `Dataset` class. A `Dataset` object takes a few parameters that will be used to setup the needed tables and its `setup` function is used to setup needed tables. For TPC-DS benchmark, the class is `TPCDS` in the package of `com.databricks.spark.sql.perf.tpcds`. For example, to setup a TPC-DS dataset, you can 

```
import org.apache.spark.sql.parquet.Tables
// Tables in TPC-DS benchmark used by experiments.
val tables = Tables(sqlContext)
// Setup TPC-DS experiment
val tpcds =
  new TPCDS (
    sqlContext = sqlContext,
    sparkVersion = "1.3.1",
    dataLocation = <the location of data>,
    dsdgenDir = <the location of dsdgen in every worker>,
    tables = tables.tables,
    scaleFactor = <scale factor>,
    includeBreakdown = false)
```

After a `TPCDS` object is created, tables of it can be setup by calling

```
tpcds.setup()
```

The `setup` function will first check if needed tables are stored at the location specified by `dataLocation`. If not, it will creates tables at there by using the data generator tool `dsdgen` provided by TPC-DS benchmark (This tool needs to be pre-installed at the location specified by `dsdgenDir` in every worker).

### Run benchmarking queries
After setup, users can use `runExperiment` function to run benchmarking queries and record query execution time. Taking TPC-DS as an example, you can start an experiment by using

```
tpcds.runExperiment(
  queries = <a Seq of Queries>,
  resultsLocation = <the root location of performance results>,
  includeBreakdown = <if measure the performance of every physical operators>,
  iterations = <the number of iterations>,
  variations = <variations used in the experiment>,
  tags = <tags of this experiment>)
```

For every experiment run (i.e.\ every call of `runExperiment`), Spark SQL Perf will use the timestamp of the start time to identify this experiment. Performance results will be stored in the sub-dir named by the timestamp in the given `resultsLocation` (for example `results/1429213883272`). The performance results are stored in the JSON format.

### Retrieve results
The follow code can be used to retrieve results ...

```
// Get experiments results.
import com.databricks.spark.sql.perf.Results
val results = Results(resultsLocation = <the root location of performance results>, sqlContext = sqlContext)
// Get the DataFrame representing all results stored in the dir specified by resultsLocation.
val allResults = results.allResults
// Use DataFrame API to get results of a single run.
allResults.filter("timestamp = 1429132621024")
```