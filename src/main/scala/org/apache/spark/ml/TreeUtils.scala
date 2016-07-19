package org.apache.spark.ml

import org.apache.spark.ml.attribute.{AttributeGroup, NominalAttribute, NumericAttribute}
import org.apache.spark.sql.DataFrame

object TreeUtils {
  /**
   * Set label metadata (particularly the number of classes) on a DataFrame.
   *
   * @param data  Dataset.  Categorical features and labels must already have 0-based indices.
   *              This must be non-empty.
   * @param featuresColName  Name of the features column
   * @param featureArity  Array of length numFeatures, where 0 indicates continuous feature and
   *                      value > 0 indicates a categorical feature of that arity.
   * @return  DataFrame with metadata
   */
  def setMetadata(
      data: DataFrame,
      labelMeta: Option[(String, Int)],
      featuresColName: String,
      featureArity: Array[Int]): DataFrame = {
    val labelCol = labelMeta.map { case (labelColName, numClasses) =>
      val labelAttribute = if (numClasses == 0) {
        NumericAttribute.defaultAttr.withName(labelColName)
      } else {
        NominalAttribute.defaultAttr.withName(labelColName).withNumValues(numClasses)
      }
      val labelMetadata = labelAttribute.toMetadata()
      data(labelColName).as(labelColName, labelMetadata)
    }
    val featuresAttributes = featureArity.zipWithIndex.map { case (arity: Int, feature: Int) =>
      if (arity > 0) {
        NominalAttribute.defaultAttr.withIndex(feature).withNumValues(arity)
      } else {
        NumericAttribute.defaultAttr.withIndex(feature)
      }
    }
    val featuresMetadata = new AttributeGroup("features", featuresAttributes).toMetadata()
    val cols = Seq(data(featuresColName).as(featuresColName, featuresMetadata)) ++ labelCol.toSeq
    data.select(cols: _*)
  }
}
