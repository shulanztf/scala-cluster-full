package com.msb.lr_new

import org.apache.spark.SparkConf
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.sql.SparkSession

/**
  * @ClassName: LinearRegression01
  * @Author: zhaotf
  * @Description:
  * @Date: 2020/2/22 0022 
  */
class LinearRegression01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")

    val spark = SparkSession.builder().config(conf).appName("LinearRegression").getOrCreate()

    val data = spark.read.format("libsvm")
      .load("data/sample_linear_regression_data.txt")

    val DFS = data.randomSplit(Array(0.8,0.2))
    val (training,test) = (DFS(0),DFS(1))

    val lr = new LinearRegression()
      .setMaxIter(10)
      .setTol(1)
    //L1+L2系数之和    0代表不使用正则化
    //      .setRegParam(0.3)
    /**
      * 用于调整L1、L2之间的比例，简单说:调整L1，L2前面的系数
      * For alpha = 0, the penalty is an L2 penalty.
      * For alpha = 1, it is an L1 penalty.
      * For alpha in (0,1), the penalty is a combination of L1 and L2.
      */
    //      .setElasticNetParam(0.8)


    // Fit the model
    val lrModel = lr.fit(training)

    // 打印模型参数w1...wn和截距w0
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    //使用模型来预测
    val predictionAndLabel = lrModel.setFeaturesCol("features").setPredictionCol("test_prediction").transform(test)
    val loss = predictionAndLabel.rdd.map(row =>{
      val label = row.getAs[Double]("label")
      val predict = row.getAs[Double]("test_prediction")
      Math.abs(label - predict)
    }).reduce(_+_)
    val error = loss / test.count
    println("Test RMSE = " + error)


    //模型保存
    val saveModelPath = "hdfs://node01:9000/mllib/model/lrmodel"
    lrModel.write.overwrite().save(saveModelPath)

    //模型加载
    val regressionModel = LinearRegressionModel.load(saveModelPath)
    regressionModel.transform(test)

    //模型一探究竟
    spark.read.parquet(saveModelPath+"/data").show(false)
  }
}
