package com.sutdy.hlht

import org.apache.spark.SparkConf
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS, LogisticRegressionWithSGD}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SparkSession

/**
  *
  */
object SVM {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    val session:SparkSession = SparkSession.builder().config(conf).appName("LinearRegression").getOrCreate()
    import session.implicits._

    // 预处理数据
    val data = MLUtils.loadLibSVMFile(session.sparkContext, "/data/spark/spark-2.3.2-bin-hadoop2.7/data/mllib/sample_libsvm_data.txt")
    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // 训练模型，迭代次数50次
    val model = LogisticRegressionWithSGD.train(training,50)

    // 测试集上测试准确率
    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }

    val accuracy = predictionAndLabels.filter(x => x._1 == x._2).count()*1.0/predictionAndLabels.count()
    println(s"Accuracy = $accuracy")
  }

}
