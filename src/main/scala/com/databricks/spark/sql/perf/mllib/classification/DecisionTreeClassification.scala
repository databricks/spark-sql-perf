package com.databricks.spark.sql.perf.mllib.classification

import org.apache.spark.ml._
import org.apache.spark.ml.classification.DecisionTreeClassifier

import com.databricks.spark.sql.perf.mllib.OptionImplicits._
import com.databricks.spark.sql.perf.mllib._

object DecisionTreeClassification extends BenchmarkAlgorithm with TreeOrForestClassifier {

  override def getPipelineStage(ctx: MLBenchContext): PipelineStage = {
    import ctx.params._
    new DecisionTreeClassifier()
      .setMaxDepth(depth)
      .setSeed(ctx.seed())
  }
}
