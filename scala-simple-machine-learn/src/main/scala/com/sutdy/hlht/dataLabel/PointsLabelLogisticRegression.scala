package com.sutdy.hlht.dataLabel

import com.sutdy.hlht.LogisticSGD.parseLine
import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithSGD}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * 积分订单标签预测,  逻辑回归 logistic regression
  * 执行准确率：0.8859556494192186
  */
object PointsLabelLogisticRegression {

  def main(args: Array[String]): Unit = {
    val conf:SparkConf = new SparkConf()
    conf.setMaster("local")
    val session:SparkSession = SparkSession.builder().config(conf).appName("PointsLabelLogisticRegression").getOrCreate()
    import session.implicits._
    /**
      * 样本数据格式，4列：
      * 是否刷分，0否，1是;
      * 自增id；
      * 发生时间段(小时)；
      * 单位时间内用户积分笔数（10秒）
      */
    val data:RDD[String] = session.sparkContext.textFile("D:\\data\\hlht\\point\\points-data-label\\points-label-train-20200319.txt")
    val dataRDD = data.map(line => {
      val array = StringUtils.split(line, "\t")
      val vector: Vector = Vectors.dense(array(2).toDouble, array(3).toDouble) //特征
      LabeledPoint(array(0).toDouble, vector)
    })

    val splits = dataRDD.randomSplit(Array(0.8, 0.2), seed=11L)
    val trainData = splits(0)//训练集
    val testData = splits(1)//测试集

    val model:LogisticRegressionModel = LogisticRegressionWithSGD.train(trainData, 50)

    println("aaa",model.weights.size)
    println("bbb",model.weights)
    println("ccc",model.weights.toArray.filter(_ != 0).size)

    val predictionAndLabel:RDD[(Double,Double)] = testData.map(p => (model.predict(p.features), p.label))//数据格式(预测标签，实际标签)
    val size = predictionAndLabel.count()
    var flg1 = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count()/size //预测准确率
    println("end....",flg1)
//    predictionAndLabel.foreach(println)
  }

}
