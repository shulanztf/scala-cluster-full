package com.sutdy.hlht
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.sql.SparkSession

/**
  * https://www.cnblogs.com/abcdwxc/p/9837349.html  K均值（K-means）算法
  */
object KmeansDemo {
  def main(args:Array[String]): Unit ={
    val spark = SparkSession
      .builder
      .appName("KmeansDemo").master("local")
//      .config("spark.sql.warehouse.dir", "C:\\study\\sparktest")
      .getOrCreate()
    // Loads data.
    val dataset=spark.read.format("libsvm").load("D:\\data\\spark\\spark-2.3.2-bin-hadoop2.7\\data\\mllib\\sample_kmeans_data.txt")
    // Trains a k-means model.
    val kmeans=new KMeans().setK(2).setSeed(1L)
    val model=kmeans.fit(dataset)

    //Make predictions
    val predictions=model.transform(dataset)

    // Evaluate clustering by computing Silhouette score
    val evaluator = new ClusteringEvaluator()

    val silhouette = evaluator.evaluate(predictions)
    println(s"Silhouette with squared euclidean distance = $silhouette")

    // Shows the result.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)

    spark.stop()
  }
}
