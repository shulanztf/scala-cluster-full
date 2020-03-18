package com.sutdy.hlht

import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * https://blog.csdn.net/lsshlsw/article/details/45174391 Spark朴素贝叶斯(naiveBayes)
  */
object NaiveBayesDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DecisionTreeDemo").setMaster("local")
    val sc = new SparkContext(conf)
    // 读入数据
    val data = sc.textFile("D:\\data\\spark\\data\\NaiveBayesDemo-data.txt")
    val parsedData = data.map(line => {
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    })
    //    把数据的60%作为训练集，40%作为测试集
    val splits = parsedData.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0)
    val test = splits(1)
    // 获得训练模型,第一个参数为数据，第二个参数为平滑参数，默认为1，可改
    val model = NaiveBayes.train(training, lambda = 1.0)
    // 对模型进行准确度分析
    val predictionAndLabel:RDD[(Double,Double)] = test.map(p => (model.predict(p.features), p.label))
    println("abc...")
    predictionAndLabel.foreach(println)
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
    println("accuracy-->" + accuracy)
    println("Predictionof (0.0, 2.0, 0.0, 1.0):" + model.predict(Vectors.dense(0.0, 2.0, 0.0, 1.0)))
  }


}
