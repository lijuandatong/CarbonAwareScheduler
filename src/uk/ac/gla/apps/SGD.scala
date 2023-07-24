package uk.ac.gla.apps

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.optimization.SquaredL2Updater
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}
object SGD {

  def main(args: Array[String]): Unit = {

    val appSignature = "SGD"

    val sparkConf = new SparkConf()
      .setAppName(appSignature)
      .setMaster("local[4]")
    //      .set("spark.shuffle.service.enabled", "true")
    //      .set("spark.dynamicAllocation.enabled", "true")

    val sparkContext = new SparkContext(sparkConf)

    // 加载和解析数据
    val data = sparkContext.textFile("data/lpsa.data")
    val parsedData = data.map { line =>
      val parts = line.split(',')
      val features = parts(1).split(' ').map(_.toDouble)
      LabeledPoint(parts(0).toDouble, Vectors.dense(features))
    }
    parsedData.cache()

//    var trainingSet = MLUtils.loadLabeledPoints(sparkContext, "")
//    if (true) {
//      trainingSet = trainingSet.cache()
//    }

    val numIterations = 8
    val stepSize = 1.0
    val regParam = 0.01

    val algorithm = new LinearRegressionWithSGD()
    algorithm.optimizer
      .setNumIterations(8)
      .setStepSize(stepSize)
      .setUpdater(new SquaredL2Updater())
      .setRegParam(regParam)

    val model = algorithm.run(parsedData)
    println(model.weights)

    // 保存第一次迭代的结果
    val weights1 = model.weights
    val intercept1 = model.intercept

//    val initialWeights = Vectors.dense(weights1.toArray ++ Array(intercept1))

//    val algorithm1 = new LinearRegressionWithSGD()
//    algorithm1.optimizer
//      .setNumIterations(4)
//      .setStepSize(stepSize)
//      .setUpdater(new SquaredL2Updater())
//      .setRegParam(regParam)
//
//    // 更新标签，以应用上一次迭代的截距
//    val updatedData = parsedData.map(x => LabeledPoint(x.label - intercept1, x.features))
//    val model1 = algorithm1.run(updatedData, weights1)
//    println(model1.weights)

    Thread.sleep(3600 * 1000)
    sparkContext.stop()
  }
}
