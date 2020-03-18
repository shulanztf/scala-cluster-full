package com.sutdy.hlht

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering._


/**
  * https://www.cnblogs.com/mstk/p/7151943.html			Spark机器学习(7)：KMenas算法
  */
object KNN {

  def main(args: Array[String]): Unit = {
      // 设置运行环境
      val conf = new SparkConf().setAppName("KMeansTest").setMaster("local")
      val sc = new SparkContext(conf)
      Logger.getRootLogger.setLevel(Level.WARN)

      // 读取样本数据并解析
      val data = sc.textFile("D:\\data\\spark\\data\\kmeans_data.txt")
      val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

      // 新建KMeans聚类模型并训练
      val initMode = "k-means||"
      val numClusters = 2
      val numIterations = 500
      val model = new KMeans().
        setInitializationMode(initMode).
        setK(numClusters).
        setMaxIterations(numIterations).
        run(parsedData)
      val centers = model.clusterCenters
      println("Centers:")
      for (i <- 0 to centers.length - 1) {
        println(centers(i)(0) + "\t" + centers(i)(1))
      }

      // 误差计算
      val Error = model.computeCost(parsedData)
      println("Errors = " + Error)
  }

}
