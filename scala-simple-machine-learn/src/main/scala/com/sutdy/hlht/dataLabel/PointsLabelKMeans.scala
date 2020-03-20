package com.sutdy.hlht.dataLabel

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 积分订单标签预测,KMeans,弃用，改用KNN
  */
object PointsLabelKMeans {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("PointsLabelKMeans").master("local").getOrCreate()
    // Loads data.
    val dataset:DataFrame =spark.read.format("libsvm").load("D:\\data\\hlht\\point\\points-data-label\\points-label-train-libsvm-20200320.txt")
    dataset.show(10)

    // Trains a k-means model.
    val kmeans=new KMeans().setK(2).setSeed(1L)
    val model=kmeans.fit(dataset)

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
