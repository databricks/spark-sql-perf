package org.apache.spark.ml

import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, LogisticRegressionModel}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.regression.{LinearRegressionModel, GeneralizedLinearRegressionModel, DecisionTreeRegressionModel}
import org.apache.spark.ml.tree._
import org.apache.spark.mllib.random.RandomDataGenerator
import org.apache.spark.mllib.tree.impurity.ImpurityCalculator


/**
 * Helper for creating MLlib models which have private constructors.
 */
object ModelBuilder {

  def newLogisticRegressionModel(
      coefficients: Vector,
      intercept: Double): LogisticRegressionModel = {
    new LogisticRegressionModel("lr", coefficients, intercept)
  }

  def newLinearRegressionModel(
      coefficients: Vector,
      intercept: Double): LinearRegressionModel = {
    new LinearRegressionModel("linr", coefficients, intercept)
  }

  def newGLR(
      coefficients: Vector,
      intercept: Double): GeneralizedLinearRegressionModel =
    new GeneralizedLinearRegressionModel("glr-uid", coefficients, intercept)

  def newDecisionTreeClassificationModel(
      depth: Int,
      numClasses: Int,
      featureArity: Array[Int],
      seed: Long): DecisionTreeClassificationModel = {
    require(numClasses >= 2, s"DecisionTreeClassificationModel requires numClasses >= 2," +
      s" but was given $numClasses")
    val rootNode = TreeBuilder.randomBalancedDecisionTree(depth = depth, labelType = numClasses,
      featureArity = featureArity, seed = seed)
    new DecisionTreeClassificationModel(rootNode, numFeatures = featureArity.length,
      numClasses = numClasses)
  }

  def newDecisionTreeRegressionModel(
      depth: Int,
      featureArity: Array[Int],
      seed: Long): DecisionTreeRegressionModel = {
    val rootNode = TreeBuilder.randomBalancedDecisionTree(depth = depth, labelType = 0,
      featureArity = featureArity, seed = seed)
    new DecisionTreeRegressionModel(rootNode, numFeatures = featureArity.length)
  }
}

/**
 * Helpers for creating random decision trees.
 */
object TreeBuilder {

  /**
   * Generator for a pair of distinct class labels from the set {0,...,numClasses-1}.
   * Pairs are useful for trees to make sure sibling leaf nodes make different predictions.
   * @param numClasses  Number of classes.
   */
  private class ClassLabelPairGenerator(val numClasses: Int)
    extends RandomDataGenerator[Pair[Double, Double]] {

    require(numClasses >= 2,
      s"ClassLabelPairGenerator given label numClasses = $numClasses, but numClasses should be >= 2.")

    private val rng = new java.util.Random()

    override def nextValue(): Pair[Double, Double] = {
      val left = rng.nextInt(numClasses)
      var right = rng.nextInt(numClasses)
      while (right == left) {
        right = rng.nextInt(numClasses)
      }
      new Pair[Double, Double](left, right)
    }

    override def setSeed(seed: Long): Unit = {
      rng.setSeed(seed)
    }

    override def copy(): ClassLabelPairGenerator = new ClassLabelPairGenerator(numClasses)
  }


  /**
   * Generator for a pair of real-valued labels.
   * Pairs are useful for trees to make sure sibling leaf nodes make different predictions.
   */
  private class RealLabelPairGenerator() extends RandomDataGenerator[Pair[Double, Double]] {

    private val rng = new java.util.Random()

    override def nextValue(): Pair[Double, Double] =
      new Pair[Double, Double](rng.nextDouble(), rng.nextDouble())

    override def setSeed(seed: Long): Unit = {
      rng.setSeed(seed)
    }

    override def copy(): RealLabelPairGenerator = new RealLabelPairGenerator()
  }

  /**
   * Creates a random decision tree structure.
   * @param depth  Depth of tree to build.  Must be <= numFeatures.
   * @param labelType  Value 0 indicates regression.  Integers >= 2 indicate numClasses for
   *                   classification.
   * @param featureArity  Array of length numFeatures indicating feature type.
   *                      Value 0 indicates continuous feature.
   *                      Other values >= 2 indicate a categorical feature,
   *                      where the value is the number of categories.
   * @return  root node of tree
   */
  def randomBalancedDecisionTree(
      depth: Int,
      labelType: Int,
      featureArity: Array[Int],
      seed: Long): Node = {
    require(depth >= 0, s"randomBalancedDecisionTree given depth < 0.")
    val numFeatures = featureArity.length
    require(depth <= numFeatures,
      s"randomBalancedDecisionTree requires depth <= featureArity.size," +
        s" but depth = $depth and featureArity.size = $numFeatures")
    val isRegression = labelType == 0
    if (!isRegression) {
      require(labelType >= 2, s"labelType must be >= 2 for classification. 0 indicates regression.")
    }

    val rng = new scala.util.Random()
    rng.setSeed(seed)

    val labelGenerator = if (isRegression) {
      new RealLabelPairGenerator()
    } else {
      new ClassLabelPairGenerator(labelType)
    }
    labelGenerator.setSeed(rng.nextLong)
    // We use a dummy impurityCalculator for all nodes.
    val impurityCalculator = if (isRegression) {
      ImpurityCalculator.getCalculator("variance", Array.fill[Double](3)(0.0))
    } else {
      ImpurityCalculator.getCalculator("gini", Array.fill[Double](labelType)(0.0))
    }

    randomBalancedDecisionTreeHelper(depth, featureArity, impurityCalculator,
      labelGenerator, Set.empty, rng)
  }

  /**
   * Create an internal node.  Either create the leaf nodes beneath it, or recurse as needed.
   * @param subtreeDepth  Depth of subtree to build.  Depth 0 means this is a leaf node.
   * @param featureArity  Indicates feature type.  Value 0 indicates continuous feature.
   *                      Other values >= 2 indicate a categorical feature,
   *                      where the value is the number of categories.
   * @param impurityCalculator  Dummy impurity calculator to use at all tree nodes
   * @param usedFeatures  Features appearing in the path from the tree root to the node
   *                      being constructed.
   * @param labelGenerator  Generates pairs of distinct labels.
   * @return
   */
  private def randomBalancedDecisionTreeHelper(
      subtreeDepth: Int,
      featureArity: Array[Int],
      impurityCalculator: ImpurityCalculator,
      labelGenerator: RandomDataGenerator[Pair[Double, Double]],
      usedFeatures: Set[Int],
      rng: scala.util.Random): Node = {

    if (subtreeDepth == 0) {
      // This case only happens for a depth 0 tree.
      return new LeafNode(prediction = 0.0, impurity = 0.0, impurityStats = impurityCalculator)
    }

    val numFeatures = featureArity.length
    // Should not happen.
    assert(usedFeatures.size < numFeatures, s"randomBalancedDecisionTreeSplitNode ran out of " +
      s"features for splits.")

    // Make node internal.
    var feature: Int = rng.nextInt(numFeatures)
    while (usedFeatures.contains(feature)) {
      feature = rng.nextInt(numFeatures)
    }
    val split: Split = if (featureArity(feature) == 0) {
      // continuous feature
      new ContinuousSplit(featureIndex = feature, threshold = rng.nextDouble())
    } else {
      // categorical feature
      // Put nCatsSplit categories on left, and the rest on the right.
      // nCatsSplit is in {1,...,arity-1}.
      val nCatsSplit = rng.nextInt(featureArity(feature) - 1) + 1
      val splitCategories: Array[Double] =
        rng.shuffle(Range(0,featureArity(feature)).toList).toArray.map(_.toDouble).take(nCatsSplit)
      new CategoricalSplit(featureIndex = feature,
        _leftCategories = splitCategories, numCategories = featureArity(feature))
    }

    val (leftChild: Node, rightChild: Node) = if (subtreeDepth == 1) {
      // Add leaf nodes.  Assign these jointly so they make different predictions.
      val predictions = labelGenerator.nextValue()
      val leftChild = new LeafNode(prediction = predictions._1, impurity = 0.0,
        impurityStats = impurityCalculator)
      val rightChild = new LeafNode(prediction = predictions._2, impurity = 0.0,
        impurityStats = impurityCalculator)
      (leftChild, rightChild)
    } else {
      val leftChild = randomBalancedDecisionTreeHelper(subtreeDepth - 1, featureArity,
        impurityCalculator, labelGenerator, usedFeatures + feature, rng)
      val rightChild = randomBalancedDecisionTreeHelper(subtreeDepth - 1, featureArity,
        impurityCalculator, labelGenerator, usedFeatures + feature, rng)
      (leftChild, rightChild)
    }
    new InternalNode(prediction = 0.0, impurity = 0.0, gain = 0.0, leftChild = leftChild,
      rightChild = rightChild, split = split, impurityStats = impurityCalculator)
  }
}
