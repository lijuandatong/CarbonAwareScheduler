package uk.ac.gla.apps

import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

object GradientBoostedTrees {
  def main(args: Array[String]): Unit = {

    val appSignature = "GBT"

    val sparkConf = new SparkConf()
      .setAppName(appSignature)
    val sparkContext = new SparkContext(sparkConf)

    sparkContext.textFile("")

    // Load and parse the data file.
    val data = MLUtils.loadLabeledPoints(sparkContext, "")
    // Split the data into training and test sets (30% held out for testing)
    val splits = data.randomSplit(Array(0.7, 0.3))

    val (trainingData, testData) = (splits(0), splits(1))

    // Train a GradientBoostedTrees model.
    // The defaultParams for Classification use LogLoss by default.
    val boostingStrategy = BoostingStrategy
      .defaultParams("Regression")
    boostingStrategy.setNumIterations(10) // Note: Use more iterations in practice.
    //    boostingStrategy.treeStrategy.setNumClasses(2)
    //    boostingStrategy.treeStrategy.setMaxDepth(5)
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    //    boostingStrategy.treeStrategy.setCategoricalFeaturesInfo(Map[Int, Int]())

    val model = org.apache.spark.mllib.tree.GradientBoostedTrees.train(trainingData, boostingStrategy)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
    println("Test Error = " + testErr)
    println("Learned classification GBT model:\n" + model.toDebugString)

  }
}

