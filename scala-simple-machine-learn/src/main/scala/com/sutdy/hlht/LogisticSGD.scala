package com.sutdy.hlht
/*
accident.txt
accident(去年是否出过事故，1表示出过事故，0表示没有)
age(年龄 数值型)
vision(视力状况，分类型，1表示好，0表示有问题)
drive(驾车教育，分类型，1表示参加过驾车教育，0表示没有)
 */
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithSGD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * https://blog.csdn.net/hjh00/article/details/72801010?depth_1-utm_source=distribute.pc_relevant.none-task&utm_source=distribute.pc_relevant.none-task
  * Spark MLlib 入门学习笔记 - 逻辑回归
  */
object LogisticSGD {

  def parseLine(line: String): LabeledPoint = {
    val parts = line.split(" ")
    val vd: Vector = Vectors.dense(parts(1).toDouble, parts(2).toDouble, parts(3).toDouble)//特征
    return LabeledPoint(parts(0).toDouble, vd )// parts(0)数据标签
  }

  def main(args: Array[String]){
    val conf = new SparkConf().setMaster("local[3]").setAppName("LogisticSGD")
    val sc = new SparkContext(conf)
    val data =  sc.textFile("D:\\data\\spark\\data\\LogisticSGD-data.txt").map(parseLine(_))

    val splits = data.randomSplit(Array(0.8, 0.2), seed=11L)
    val trainData = splits(0)//训练集
    val testData = splits(1)//测试集

    val model:LogisticRegressionModel = LogisticRegressionWithSGD.train(trainData, 50)

    println("aaa",model.weights.size)
    println("bbb",model.weights)
    println("ccc",model.weights.toArray.filter(_ != 0).size)

    val predictionAndLabel:RDD[(Double,Double)] = testData.map(p => (model.predict(p.features), p.label))//数据格式(预测标签，实际标签)

    println("abc....")
    predictionAndLabel.foreach(println)

  }

}
