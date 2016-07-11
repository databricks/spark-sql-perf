package org.apache.spark.ml

import org.apache.spark.ml.attribute.{AttributeGroup, NominalAttribute, NumericAttribute}
import org.apache.spark.sql.DataFrame

object TreeUtils {
  /**
   * Set label metadata (particularly the number of classes) on a DataFrame.
   *
   * @param data  Dataset.  Categorical features and labels must already have 0-based indices.
   *              This must be non-empty.
   * @param labelColName  Name of the label column on which to set the metadata.
   * @param numClasses  Number of classes label can take. If 0, mark as continuous.
   * @param featuresColName  Name of the features column
   * @param featureArity  Array of length numFeatures, where 0 indicates continuous feature and
   *                      value > 0 indicates a categorical feature of that arity.
   * @return  DataFrame with metadata
   */
  def setMetadata(
      data: DataFrame,
      labelColName: String,
      numClasses: Int,
      featuresColName: String,
      featureArity: Array[Int]): DataFrame = {
    val labelAttribute = if (numClasses == 0) {
      NumericAttribute.defaultAttr.withName(labelColName)
    } else {
      NominalAttribute.defaultAttr.withName(labelColName).withNumValues(numClasses)
    }
    val labelMetadata = labelAttribute.toMetadata()
    val featuresAttributes = featureArity.zipWithIndex.map { case (arity: Int, feature: Int) =>
      if (arity > 0) {
        NominalAttribute.defaultAttr.withIndex(feature).withNumValues(arity)
      } else {
        NumericAttribute.defaultAttr.withIndex(feature)
      }
    }
    val featuresMetadata = new AttributeGroup("features", featuresAttributes).toMetadata()
    data.select(data(featuresColName).as(featuresColName, featuresMetadata),
      data(labelColName).as(labelColName, labelMetadata))
  }
}
