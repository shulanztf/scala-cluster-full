package com.sutdy.hlht.dataLabel

import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * 积分订单标签预测,朴素贝叶斯
  * 准确率：0.9662090813093981
  */
object PointsLabelNaiveBayes {

  def main(args: Array[String]): Unit = {
    val conf:SparkConf = new SparkConf()
    conf.setMaster("local")
    val session:SparkSession = SparkSession.builder().config(conf).appName("PointsLabelNaiveBayes").getOrCreate()

//    val conf = new SparkConf().setAppName("DecisionTreeDemo").setMaster("local")
//    val sc = new SparkContext(conf)
    // 读入数据
    val data = session.sparkContext.textFile("D:\\data\\hlht\\point\\points-data-label\\points-label-train-20200319.txt")
    val parsedData = data.map(line => {
//      val parts = line.split(',')
//      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
      val array = StringUtils.split(line, "\t")
      val vector: Vector = Vectors.dense(array(2).toDouble, array(3).toDouble) //特征
      LabeledPoint(array(0).toDouble, vector)
    })
    //    把数据的80%作为训练集，20%作为测试集
    val splits = parsedData.randomSplit(Array(0.8, 0.2), seed = 11L)
    val training = splits(0)
    val test = splits(1)
    // 获得训练模型,第一个参数为数据，第二个参数为平滑参数，默认为1，可改
    val model = NaiveBayes.train(training, lambda = 1.0)
    // 对模型进行准确度分析
    val predictionAndLabel:RDD[(Double,Double)] = test.map(p => (model.predict(p.features), p.label))
//    println("abc...")
//    predictionAndLabel.foreach(println)
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
    println("accuracy:", accuracy)//准备率
//    println("Predictionof (0.0, 2.0, 0.0, 1.0):" + model.predict(Vectors.dense(0.0, 2.0, 0.0, 1.0)))

    session.close()
  }

}
