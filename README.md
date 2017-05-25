# Spark SQL Performance Tests

[![Build Status](https://travis-ci.org/databricks/spark-sql-perf.svg)](https://travis-ci.org/databricks/spark-sql-perf)

This is a performance testing framework for [Spark SQL](https://spark.apache.org/sql/) in [Apache Spark](https://spark.apache.org/) 1.6+. The toolkit includes the TPC-DS benchmark and a few other sample benchmarks.

# Build

Suppose the spark-sql-perf tool is located at ~/spark-sql-perf. The (non-intuitive) command to build the benchmark is:

```
cd ~/spark-sql-perf
bin/run --help
```

The first time you run the command, it may take a while because it may download and install dependencies such as sbt. Future runs should be faster.

# Run a Benchmark

Have a look at src/main/scala/com/databricks/spark/sql/perf/DatasetPerformance.scala. It defines a class `DatasetPerformance` which derives from Benchmark. To run this particular benchmark:

```
cd ~/spark-sql-perf
bin/run --benchmark DatasetPerformance
```

# The TPC-DS Benchmark

## Build dsdgen

This step builds a data generation tool that will be called by `tables.genData()` in a later step.
Download TPC-DS Tools from http://tpc.org.
Another option is to use Databricks' fork of dsdgen [here](https://github.com/davies/tpcds-kit).

Suppose the downloaded zip file is ~/tpc_ds/127d3fa4-fac9-43f7-b1c5-6a00c2291ab9-tpc-ds-tool.zip, and the version is v2.4.0. To unzip and build:

```
cd ~/tpc_ds
unzip 127d3fa4-fac9-43f7-b1c5-6a00c2291ab9-tpc-ds-tool.zip
cd v2.4.0/tools
make
```

If the build is successful, you should get an executable `dsdgen`.

**Note:** It may be necessary to change `ReportErrorNoLine(..., 1)` to `ReportErrorNoLine(..., 0)` in line 492 of driver.c, to avoid a error when running `tables.genData()` at a later step with error message like "Table ... is a child; it is populated during the build of its parent (e.g., catalog_sales builds catalog returns)."

**Note for Mac:** The dsdgen tool does not build on Mac. But you can easily make it build. Essentially, you should include `stdlib.h` instead of `malloc.h`, include `limits.h` instead of `values.h`, and `#define MAXINT INT_MAX`.

## Build Spark

You may get Spark from http://github.com/apache/spark.

Suppose the spark directory is ~/spark. To build:

```
cd ~/spark
build/mvn -DskipTests -T 8C -Dskip -Phive -Phive-thriftserver package
```

In the above command, `-T 8C` means to use 8 threads to build; `-Phive -Phive-thriftserver` means to build with Hive support.

## Start a spark-shell with access to spark-sql-perf classes

Supose the previous step that builds spark-sql-perf generated a folder scala-2.11 in ~/spark-sql-perf/target.

```
cd ~/spark
bin/spark-shell --driver-class-path ~/spark-sql-perf/target/scala-2.11/classes
```

## Generate data, create tables, and create a TPCDS object

Run the following commands in the spark-shell.

```
import com.databricks.spark.sql.perf.tpcds.Tables
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

val sqlContext = SQLContext.getOrCreate(sc)

// Create a Tables object without generating any data yet.
// Record the locatin of dsdgen, and scale factor 1.
val tables = new Tables(sqlContext, "~/tpc_ds/v2.4.0/tools", 1)

// Call dsdgen to create .dat files (if not exists already) in ~/tpc_ds/v2.4.0/tools.
// Then load into parquet files to be stored at "~/datafiles".
// The five boolean parameters are:
// - true  = overwrite
// - false = partitionTables
// - true  = useDoubleForDecimal
// - false = clusterByPartitionColumns
// - false = filterOutNullPartitionValues
// See the source code src/main/scala/com/databricks/spark/sql/perf/tpcds/Tables.scala.
tables.genData("~/datafiles", "parquet", true, false, true, false, false)

// Generate tables using the parquet files at ~/datafiles, in a database "tpcds".
// The last parameter means to overwrite.
// Note that you could also create temporary tables. Again see the source code.
tables.createExternalTables("~/datafiles", "parquet", "tpcds", true)

// Create a TPCDS object.
import com.databricks.spark.sql.perf.tpcds.TPCDS
val tpcds = new TPCDS(sqlContext = sqlContext)
```

## Run benchmarking queries
After setup, users can use `runExperiment()` function to run benchmarking queries and record query execution time. Taking TPC-DS as an example, you can start an experiment by using

```
val experiment = tpcds.runExperiment(tpcds.interactiveQueries)
```

For every experiment run (i.e. every call of `runExperiment`), Spark SQL Perf will use the timestamp of the start time to identify this experiment. Performance results will be stored in the sub-dir named by the timestamp in the given `resultsLocation` (for example `results/1429213883272`). The performance results are stored in the JSON format.

## Retrieve results
While the experiment is running you can use `experiment.html` to list the status.  Once the experiment is complete, the results will be saved to the table sqlPerformance in json.

```
// Get all experiments results.
tpcds.createResultsTable()
sqlContext.table("sqlPerformance")

// Get the result of a particular run by specifying the timestamp of that run.
sqlContext.table("sqlPerformance").filter("timestamp = 1429132621024")
```
