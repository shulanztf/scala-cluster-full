package com.sutdy.hlht

import org.apache.spark.ml.classification.{LinearSVC, LinearSVCModel}
import org.apache.spark.sql.SparkSession

/**
  * SVC=Support Vector Classification就是支持向量机用于分类
  */
object LinearSVCExample {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("LinearSVCExample").master("local")
      .getOrCreate()

    // $example on$
    // Load training data
    val training = spark.read.format("libsvm").load("D:\\data\\spark\\spark-2.3.2-bin-hadoop2.7\\data\\mllib\\sample_libsvm_data.txt")
//    println("data", training)

    //可线性分类SVM
    val lsvc = new LinearSVC()
      .setMaxIter(10)//最大迭代次数。
      .setRegParam(0.1)// 设置正则化参数

    // Fit the model
    val lsvcModel:LinearSVCModel = lsvc.fit(training)

    // Print the coefficients and intercept for linear svc
    println(s"Coefficients: ${lsvcModel.coefficients} Intercept: ${lsvcModel.intercept}")
//    println("abc",)
    lsvcModel.params.foreach(println)
    // $example off$

    spark.stop()
  }

}
