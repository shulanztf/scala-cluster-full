package com.msb.lr_new

import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.sql.SparkSession

//有无截距
object LogisticRegression02 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    val spark = SparkSession.builder().config(conf).appName("LinearRegression").getOrCreate()
    val data = spark.read.format("libsvm")
      .load("scala-simple-machine-learn/data/w0测试数据.txt")
    val splits = data.randomSplit(Array(0.7, 0.3), seed = 11L)// 拆分数据集，70%训练，30%测试
    val (trainingData, testData) = (splits(0), splits(1))// trainingData训练集，testData测试集

    val lr:LogisticRegression = new LogisticRegression()//生成逻辑回归
      .setMaxIter(10)
      //是否有截距  有解决  w0 可以 非0   w0=0
      .setFitIntercept(true)
    val lrModel:LogisticRegressionModel = lr.fit(trainingData)//生成逻辑回归模型

    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    //测试集验证正确率
    val testRest = lrModel.transform(testData)
    //打印结果
    testRest.show(false)
    //计算正确率
    val mean = testRest.rdd.map(row => {
      val label = row.getAs[Double]("label")
      val prediction = row.getAs[Double]("prediction")//prediction为固定写法
      math.abs(label - prediction)
    }).sum()

    println("正确率：" + (1-(mean/testData.count())))
    println("正确率：" + lrModel.evaluate(testData).accuracy)
    spark.close()
  }
}
