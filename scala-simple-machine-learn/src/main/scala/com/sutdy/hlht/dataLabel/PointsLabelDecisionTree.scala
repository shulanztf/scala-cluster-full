package com.sutdy.hlht.dataLabel

import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * 积分订单标签预测,决策树
  * 准确率 100%
  */
object PointsLabelDecisionTree {

  def main(args: Array[String]): Unit = {
    val conf:SparkConf = new SparkConf()
    conf.setMaster("local")
    val session:SparkSession = SparkSession.builder().config(conf).appName("PointsLabelDecisionTree").getOrCreate()

    /**
      * 样本数据格式，4列：
      * 是否刷分，0否，1是;
      * 自增id；
      * 发生时间段(小时)；
      * 单位时间内用户积分笔数（10秒）
      */
    val data:RDD[String] = session.sparkContext.textFile("D:\\data\\hlht\\point\\points-data-label\\points-label-train-20200319.txt")
    val splits:Array[RDD[LabeledPoint]] = data.map(line => {
      val array = StringUtils.split(line, "\t")
      val vector: Vector = Vectors.dense(array(2).toDouble, array(3).toDouble) //特征
      LabeledPoint(array(0).toDouble, vector)
    }).randomSplit(Array(0.8, 0.2), seed=11L)
//    val splits = dataRDD.randomSplit(Array(0.8, 0.2), seed=11L)
    val trainingData = splits(0)//训练集
    val testData = splits(1)//测试集

    //分类
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"

    //最大深度
    val maxDepth = 5
    //最大分支
    val maxBins = 32

    //模型训练
    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)

    //模型预测
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)//真实值，预测值
    }

    //树的正确率
    val testErr = 1.0 * labelAndPreds.filter(r => r._1 == r._2).count / testData.count()
    println("Test Correct:" + testErr)

    //打印树的判断值
    println("Learned classification tree model:\n" + model.toDebugString)

    session.close()
  }

}
