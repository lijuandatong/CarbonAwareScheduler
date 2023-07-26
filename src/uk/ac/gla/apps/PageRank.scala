package uk.ac.gla.apps

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, GraphLoader, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD

object PageRank {

  def main(args: Array[String]) {

    val appSignature = "PageRank"

    val sparkConf = new SparkConf()
      .setAppName(appSignature)
      .setMaster("local[4]")
    val sparkContext = new SparkContext(sparkConf)

    // 创建一个 RDD，包含顶点
    val vertices: RDD[(VertexId, String)] = sparkContext.parallelize(Array(
      (1L, "A"),
      (2L, "B"),
      (3L, "C"),
      (4L, "D")
    ))

    // 创建一个 RDD，包含边
    val edges: RDD[Edge[Double]] = sparkContext.parallelize(Array(
      Edge(1L, 2L, 1.0),
      Edge(2L, 3L, 1.0),
      Edge(3L, 4L, 1.0),
      Edge(4L, 1L, 1.0)
    ))

    val graph = GraphLoader.edgeListFile(sparkContext, "data/pagerank_data.txt")
//    val pr = graph.staticPageRank(conf.iterations())

    // 创建图
//    val graph = Graph(vertices, edges)


    // 计算 PageRank
    val result = graph.staticPageRank(10)

    result.vertices.collect().foreach { case (id, rank) =>
      println(s"Node $id has rank: $rank")
    }

    sparkContext.stop()

  }
}
