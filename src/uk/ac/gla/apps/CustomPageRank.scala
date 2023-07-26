package uk.ac.gla.apps

import org.apache.spark.{SparkConf, SparkContext}

object CustomPageRank {
  def main(args: Array[String]) {
    val appSignature = "PageRank"

    val sparkConf = new SparkConf()
      .setAppName(appSignature)
      .setMaster("local[4]")
    val sparkContext = new SparkContext(sparkConf)

    val iter = 10
    val links = sparkContext.textFile("data/web-Google.txt").map { line =>
      val parts = line.split("\t")
      (parts(0), parts(1))
    }.distinct().groupByKey().cache()

    links.foreach(println)

    var ranks = links.mapValues(v => 1.0)
    ranks.foreach(println)

    for (i <- 1 to iter) {
      val contributions = links.join(ranks).values.flatMap { case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contributions.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
      ranks.foreach(println)
    }

    sparkContext.stop()
  }
}
