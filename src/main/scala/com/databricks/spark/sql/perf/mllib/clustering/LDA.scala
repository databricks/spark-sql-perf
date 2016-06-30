package com.databricks.spark.sql.perf.mllib.clustering

import scala.collection.mutable.{HashMap => MHashMap}

import org.apache.commons.math3.random.Well19937c

import org.apache.spark.ml.Estimator
import org.apache.spark.ml
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.ml.linalg.{Vector, Vectors}

import com.databricks.spark.sql.perf.mllib.{BenchmarkAlgorithm, MLBenchContext, TestFromTraining}
import com.databricks.spark.sql.perf.mllib.OptionImplicits._


object LDA extends BenchmarkAlgorithm with TestFromTraining {
  // The LDA model is package private, no need to expose it.

  override def trainingDataSet(ctx: MLBenchContext): DataFrame = {
    import ctx.params._
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

  override def getEstimator(ctx: MLBenchContext): Estimator[_] = {
    import ctx.params._
    new ml.clustering.LDA()
      .setK(k)
      .setSeed(randomSeed.toLong)
      .setMaxIter(maxIter)
      .setOptimizer(optimizer)
  }

  // TODO(?) add a scoring method here.
}
