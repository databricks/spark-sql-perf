package com.databricks.spark.sql.perf.mllib.clustering

import com.databricks.spark.sql.perf.mllib.{ClassificationContext, TestFromTraining, ClassificationPipelineDescription}
import com.databricks.spark.sql.perf.mllib.OptionImplicits._
import org.apache.commons.math3.random.Well19937c
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.clustering.{LDA => ML_LDA}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.ml.linalg.{Vectors, Vector}
import scala.collection.mutable.{HashMap => MHashMap}

object LDA extends ClassificationPipelineDescription with TestFromTraining {
  // The LDA model is package private, no need to expose it.
  override type Model = Transformer

  def trainingDataSet(ctx: ClassificationContext): DataFrame = {
    import ctx.commonParams._
    import ctx.extraParams._
    val rdd = ctx.sqlContext.sparkContext.parallelize(
      0L until numExamples,
      numPartitions
    )
    val seed: Int = randomSeed
    val docLength = ldaDocLength.get
    val numVocab = ldaNumVocabulary.get
    val data: RDD[(Long, Vector)] = rdd.mapPartitionsWithIndex { (idx, partition) =>
      val rng = new Well19937c(seed ^ idx)
      partition.map { docIndex =>
        var currentSize = 0
        val entries = MHashMap[Int, Int]()
        while (currentSize < docLength) {
          val index = rng.nextInt(numVocab)
          entries(index) = entries.getOrElse(index, 0) + 1
          currentSize += 1
        }

        val iter = entries.toSeq.map(v => (v._1, v._2.toDouble))
        (docIndex, Vectors.sparse(numVocab, iter))
      }
    }
    ctx.sqlContext.createDataFrame(data).toDF("docIndex", "features")
  }

  def train(ctx: ClassificationContext,
            trainingSet: DataFrame): Model = {
    import ctx.commonParams._
    import ctx.extraParams._
    new ML_LDA()
        .setK(ldaNumTopics)
        .setSeed(randomSeed.toLong)
        .setMaxIter(numIterations)
        .setOptimizer(ldaOptimizer)
        .fit(trainingSet)
  }

  def score(
      ctx: ClassificationContext,
      testSet: DataFrame, model: Model): Double = -1.0
}