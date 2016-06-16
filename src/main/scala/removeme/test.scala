package removeme

object Test {
  import com.databricks.spark.sql.perf._
  import com.databricks.spark.sql.perf.mllib._
  import org.apache.spark.SparkContext
  val ctx = new com.databricks.spark.sql.perf.mllib.MLLib()
  val sc = SparkContext.getOrCreate()
  val params = MLTestParameters(Some(10), Some(10), Some(0))
  val test = new LogisticRegressionTest(sc, params, ExecutionMode.CollectResults)
  ctx.runExperiment(Seq(test))
}