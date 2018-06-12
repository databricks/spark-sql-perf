package com.databricks.spark.sql.perf.mllib.feature

/** Trait defining common state/methods for featurizers taking a single input col */
private[feature] trait UnaryTransformer {
  private[feature] val inputCol = "inputCol"
  private[feature] val outputCol = "outputCol"
}
