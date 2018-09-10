// Databricks notebook source
// TPCH runner (from spark-sql-perf) to be used on existing tables
// edit the main configuration below

val scaleFactors = Seq(1, 10, 100, 1000) //set scale factors to run
val format = "parquet" //format has have already been generated

def perfDatasetsLocation(scaleFactor: Int, format: String) = 
  s"s3a://my-bucket/tpch/sf${scaleFactor}_${format}"

val resultLocation = "s3a://my-bucket/results"
val iterations = 2
def databaseName(scaleFactor: Int, format: String) = s"tpch_sf${scaleFactor}_${format}"
val randomizeQueries = false //to use on concurrency tests

// Experiment metadata for results, edit if outside Databricks
val configuration = "default" //use default when using the out-of-box config
val runtype = "TPCH run" // Edit
val workers = 10 // Edit to the number of worker
val workerInstanceType = "my_VM_instance" // Edit to the instance type

// Make sure spark-sql-perf library is available (use the assembly version)
import com.databricks.spark.sql.perf.tpch._
import org.apache.spark.sql.functions._

// default config (for all notebooks)
var config : Map[String, String] = Map (
  "spark.sql.broadcastTimeout" -> "7200" // Enable for SF 10,000
)
// Set the spark config
for ((k, v) <- config) spark.conf.set(k, v)
// Print the custom configs first
for ((k,v) <- config) println(k, spark.conf.get(k))
// Print all for easy debugging
print(spark.conf.getAll)

val tpch = new TPCH(sqlContext = spark.sqlContext)

// filter queries (if selected)
import com.databricks.spark.sql.perf.Query
import com.databricks.spark.sql.perf.ExecutionMode.CollectResults
import org.apache.commons.io.IOUtils

val queries = (1 to 22).map { q =>
  val queryContent: String = IOUtils.toString(
    getClass().getClassLoader().getResourceAsStream(s"tpch/queries/$q.sql"))
  new Query(s"Q$q", spark.sqlContext.sql(queryContent), description = s"TPCH Query $q",
    executionMode = CollectResults)
}

// COMMAND ----------

scaleFactors.foreach{ scaleFactor =>
  println("DB SF " + databaseName(scaleFactor, format))
  sql(s"USE ${databaseName(scaleFactor, format)}")
  val experiment = tpch.runExperiment(
   queries,
   iterations = iterations,
   resultLocation = resultLocation,
   tags = Map(
   "runtype" -> runtype,
   "date" -> java.time.LocalDate.now.toString,
   "database" -> databaseName(scaleFactor, format),
   "scale_factor" -> scaleFactor.toString,
   "spark_version" -> spark.version,
   "system" -> "Spark",
   "workers" -> workers,
   "workerInstanceType" -> workerInstanceType,
   "configuration" -> configuration
   )
  )
  println(s"Running SF $scaleFactor")
  experiment.waitForFinish(36 * 60 * 60) //36hours
  val summary = experiment.getCurrentResults
  .withColumn("Name", substring(col("name"), 2, 100))
  .withColumn("Runtime", (col("parsingTime") + col("analysisTime") + col("optimizationTime") + col("planningTime") + col("executionTime")) / 1000.0)
  .select('Name, 'Runtime)
  summary.show(9999, false)
}