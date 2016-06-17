package removeme

import com.databricks.spark.sql.perf.mllib.YamlConfig
import org.apache.spark.SparkContext

object Test {
  import com.databricks.spark.sql.perf._
  import com.databricks.spark.sql.perf.mllib._
  import org.apache.spark.SparkContext
  val ctx = new com.databricks.spark.sql.perf.mllib.MLLib()
  val sc = SparkContext.getOrCreate()
  val params = MLTestParameters(Some(10), Some(10), Some(0))
  val test = new LogisticRegressionTest(sc, params, ExecutionMode.CollectResults)
  ctx.runExperiment(Seq(test))

  removeme.Do.f()


  val b = new com.databricks.spark.sql.perf.mllib.MLLib()
  val benchmarks = com.databricks.spark.sql.perf.mllib.MLBenchmarks.benchmarkObjects
  val e = b.runExperiment(
    executionsToRun = benchmarks,
    resultLocation = "/test/results")

  // Get the data:
}

object Do {
  def f(): Unit = {
    val sc = SparkContext.getOrCreate()
    sc.setLogLevel("INFO")
    val b = new com.databricks.spark.sql.perf.mllib.MLLib()
    val benchmarks = com.databricks.spark.sql.perf.mllib.MLBenchmarks.benchmarkObjects
    val e = b.runExperiment(
      executionsToRun = benchmarks,
      resultLocation = "/tmp/test/results")
    e.waitForFinish(10000)
    e.getCurrentResults()
  }

  def printErrors(): Unit = {
    val sc = SparkContext.getOrCreate()
    val sql = org.apache.spark.sql.SQLContext.getOrCreate(sc)
    import sql.implicits._
    // This is killing spark
    //  val ds = sql.sql("select * from currentResults")
    //    .as[com.databricks.spark.sql.perf.BenchmarkResult]
    val ds = sql.sql("select failure.className as className, failure.message as message from " +
      "currentResults").as[com.databricks.spark.sql.perf.Failure]
    print(ds.collect().head.message)

  }

  def printResults(): Unit = {
    val sc = SparkContext.getOrCreate()
    val sql = org.apache.spark.sql.SQLContext.getOrCreate(sc)
    // This is killing spark
    //  val ds = sql.sql("select * from currentResults")
    //    .as[com.databricks.spark.sql.perf.BenchmarkResult]
    val ds = sql.sql("select * from currentResults")
    ds.show()
  }

  def g(): Unit = {
    YamlConfig.readFile("/Users/tjhunter/work/spark-sql-perf/config.yaml")
  }
}