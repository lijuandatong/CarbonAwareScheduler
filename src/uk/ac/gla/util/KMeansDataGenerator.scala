package uk.ac.gla.util

import java.io.PrintWriter

import breeze.linalg.DenseVector
import breeze.stats.distributions.Rand

case class MeanConf(mean: DenseVector[Double], stdDev: Double)

object KMeansDataGenerator {

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("KMeansDataGenerator <samples> <cluster> <output>")
      System.exit(1)
    }

    val n = args(0).toInt
    val k = args(1).toInt
    val outputPath = args(2)

    val dim = 2
    val stdDev = .012

    Rand.generator.setSeed(0)

    val centers = uniformRandomCenters(dim, k, stdDev)
    val centerDistribution = Rand.choose(centers)
    centers.foreach(center => println(center))

    val writer = new PrintWriter(outputPath)
    (1 to n).foreach(_ => {
      val MeanConf(mean, stdDev) = centerDistribution.draw()
      val p = mean + DenseVector.rand[Double](mean.length, Rand.gaussian(0, stdDev))
      writer.println(p.toArray.mkString(" "))
    })
    writer.flush()
    writer.close()
  }

  def uniformRandomCenters(dim: Int, k: Int, stdDev: Double): Seq[MeanConf] = {
    (1 to k).map(_ => {
      val mean = DenseVector.rand[Double](dim)
      MeanConf(mean, stdDev)
    })
  }
}
