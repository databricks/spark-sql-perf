package com.databricks.spark.sql.perf.mllib

import org.apache.spark.sql.SQLContext

import scala.reflect.runtime.universe._

import com.databricks.spark.sql.perf.MLTestParameters

/*
case class MLBenchmark[Param, Model](
    common: MLTestParameters,
    extra: Param)

object MLBenchmarkRegister {
  var tests: Map[String, ClassificationPipelineDescription[Any, Any]] = Map.empty

  def register[Param: TypeTag, Model](c: ClassificationPipelineDescription[Param, Model]): Unit = {
    ???
  }

}

object MLBenchmarks {
  val benchmarks: List[MLBenchmark[_, _]] = Nil

  val sqlContext: SQLContext = ???

  val benchmarkObjects: List[MLClassificationBenchmarkable[Any, Any]] = benchmarks.map { mlb =>
    val d = MLBenchmarkRegister.tests.get(mlb.extra.getClass.getCanonicalName).get
    new MLClassificationBenchmarkable(mlb.extra, mlb.common, d, sqlContext)
  }
}
*/
