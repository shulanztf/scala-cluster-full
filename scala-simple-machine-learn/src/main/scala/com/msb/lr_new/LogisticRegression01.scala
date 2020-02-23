package com.msb.lr_new

import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


object LogisticRegression01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    val spark = SparkSession.builder().config(conf).appName("LinearRegression").getOrCreate()
    import spark.implicits._

    val dataRDD:RDD[String] = spark.sparkContext.textFile("scala-simple-machine-learn/data/breast_cancer.csv")
    val data:DataFrame = dataRDD.map(x=>{
      val arr = x.split(",")
      val features = new Array[String](arr.length-1)
      arr.copyToArray(features,0,arr.length-1)//数据文档，尾列之前的，是特征数据
      val label = arr(arr.length - 1)//数据文档，尾列，是标签数据
      (new DenseVector(features.map(_.toDouble)),label.toDouble) //生成向量二元组，（特征，标签）
    }).toDF("features","label")


    val splits:Array[Dataset[Row]] = data.randomSplit(Array(0.7, 0.3), seed = 11L)// 拆分数据集，70%训练，30%测试
    val (trainingData, testData) = (splits(0), splits(1))// trainingData训练集，testData测试集


    val lr:LogisticRegression = new LogisticRegression() //生成逻辑回归
      .setMaxIter(10)
    val lrModel:LogisticRegressionModel = lr.fit(trainingData)//生成逻辑回归模型

    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")


    //    lrModel.setThreshold(0.3)


    //测试集验证正确率
    val testRest:DataFrame = lrModel.transform(testData)
    //打印结果
    testRest.show(false)
    //计算正确率
    val mean:Double = testRest.rdd.map(row => {
      //这个样本真实的分类号
      val label = row.getAs[Double]("label")
      //将测试数据的x特征带入到model后预测出来的分类号
      val prediction = row.getAs[Double]("prediction")//prediction为固定写法
      //0:预测正确   1:预测错了
      math.abs(label - prediction)//取绝对值，分类号只有0、1
    }).sum()//计算预测错误的count值

    println("测试集正确率：" + (1-(mean/testData.count())))

    println("正确率：" + lrModel.evaluate(testData).accuracy)

  //在特定场合  要自定义分类阈值
    val count:Double = testRest.rdd.map(row => {
      val probability = row.getAs[DenseVector]("probability")
      val label = row.getAs[Double]("label")
      val score = probability(1)
      val prediction = if(score > 0.3) 1 else 0
      math.abs(label - prediction)
    }).sum()
    println("自定义分类阈值 正确率：" + (1 - (count / testData.count())))


    spark.close()//关闭资源
  }
}
