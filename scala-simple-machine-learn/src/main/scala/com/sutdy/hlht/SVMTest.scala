package com.sutdy.hlht

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.util.MLUtils


/**
  * https://www.cnblogs.com/mstk/p/7124148.html Spark机器学习(5)：SVM算法
  */
object SVMTest {

  def main(args: Array[String]): Unit = {
//    // 设置运行环境
//    val conf = new SparkConf().setAppName("SVMTest").setMaster("local")
//    val sc = new SparkContext(conf)
//    Logger.getRootLogger.setLevel(Level.WARN)
//
//    // 读取样本数据并解析
//    val dataRDD = MLUtils.loadLibSVMFile(sc, "D:\\data\\spark\\spark-2.3.2-bin-hadoop2.7\\data\\mllib\\sample_svm_data.txt")
//    // 样本数据划分,训练样本占0.8,测试样本占0.2
//    val dataParts = dataRDD.randomSplit(Array(0.8, 0.2))
//    val trainRDD = dataParts(0)
//    val testRDD = dataParts(1)
//
//    // 建立模型并训练
//    val numIterations = 100
//    val model = SVMWithSGD.train(trainRDD, numIterations)
//
//    // 对测试样本进行测试
//    val predictionAndLabel = testRDD.map { point =>
//      val score = model.predict(point.features)
//      (score, point.label, point.features)
//    }
//    val showPredict = predictionAndLabel.take(50)
//    println("Prediction" + "\t" + "Label" + "\t" + "Data")
//    for (i <- 0 to showPredict.length - 1) {
//      println(showPredict(i)._1 + "\t" + showPredict(i)._2 + "\t" + showPredict(i)._3)
//    }
//
//    // 误差计算
//    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / testRDD.count()
//    println("Accuracy = " + accuracy)
  }

}
