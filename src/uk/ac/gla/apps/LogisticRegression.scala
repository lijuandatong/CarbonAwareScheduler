package uk.ac.gla.apps

import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.util.MLUtils

object LogisticRegression {
  def main(args: Array[String]): Unit = {

    val appSignature = "LR"

    val sparkConf = new SparkConf()
      .setAppName(appSignature)
    //      .set("spark.shuffle.service.enabled", "true")
    //      .set("spark.dynamicAllocation.enabled", "true")

    val sparkContext = new SparkContext(sparkConf)


    var data = MLUtils.loadLabeledPoints(sparkContext, "")
    if (true) {
      data = data.cache()
    }
    // Split data into training (60%) and test (40%).
    val Array(training, test) = data.randomSplit(Array(0.8, 0.2))

    // Run training algorithm to build the model
    val lr = new LogisticRegressionWithLBFGS()
      .setNumClasses(3)
    lr.optimizer
      .setNumIterations(10)
      .setConvergenceTol(Double.MinPositiveValue)
    val model = lr.run(training)

    // Compute raw scores on the test set.
    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }

    // Get evaluation metrics.
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val accuracy = metrics.accuracy
    println(s"Accuracy = $accuracy")
  }
}
