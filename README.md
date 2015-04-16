## Spark SQL Performance Tests (WIP)

### TPC-DS
```
import com.databricks.spark.sql.perf.tpcds.TPCDS
import org.apache.spark.sql.parquet.Tables

// Tables used for TPC-DS.
val tables = Tables(sqlContext)

// Setup TPC-DS experiment
val tpcds =
new TPCDS (
    sqlContext = sqlContext,
    sparkVersion = "1.3.1",
    dataLocation = <the location of data>,
    dsdgenDir = <the location of dsdgen in every worker>,
    resultsLocation = <the location of performance results>,
    tables = tables.tables,
    scaleFactor = "2",
    collectResults = true)
tpcds.setupExperiment()

// Take a look at the size of every table.
tpcds.allStats.show

// Get all of the queries.
import com.databricks.spark.sql.perf.tpcds.Queries
// Just pick a single query as an example.
val oneQuery = Seq(Queries.q7Derived.head)
// Start the experiment.
val runningExp = tpcds.runExperiment(queries = oneQuery, iterations = 1)

// Get experiments results.
import com.databricks.spark.sql.perf.Results
val results = Results(resultsLocation = <the location of performance results>, sqlContext = sqlContext)
// This is all results.
val allResults = results.allResults
allResults.registerTempTable("results")
// This is the result for a single experiment started at the timestamp represented by 1429132621024 (2015-04-15 14:17:01.024). 
allResults.filter("timestamp = 1429132621024")
```
