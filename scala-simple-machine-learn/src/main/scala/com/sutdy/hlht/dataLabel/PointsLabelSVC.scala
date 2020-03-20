package com.sutdy.hlht.dataLabel

import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LinearSVC
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{PCA, StandardScaler, VectorAssembler}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 积分订单标签预测,SVC支持向量机,难度过大，暂不使用
  */
object PointsLabelSVC {

  def main(args: Array[String]): Unit = {
    // 创建sparkSession
//    val conf:SparkConf = new SparkConf()
//    conf.setMaster("local")
////    conf.setAppName()
//    val session:SparkSession = SparkSession.builder().config(conf).appName("PointsLabelSVC").getOrCreate()
//    // 加载训练数据，生成DataFrame
//    val data:DataFrame = session.read.format("libsvm").load("D:\\data\\spark\\spark-2.3.2-bin-hadoop2.7\\data\\mllib\\sample_libsvm_data.txt")
////    val data:DataFrame = session.read.load("D:\\\\data\\\\spark\\\\spark-2.3.2-bin-hadoop2.7\\\\data\\\\mllib\\\\sample_libsvm_data.txt")
//    data.show(10);

//    println(data.count())
//
//    // 归一化
//    val scaler = new StandardScaler()
//      .setInputCol("features")
//      .setOutputCol("scaledFeatures")
//      .setWithMean(true)
//      .setWithStd(true)
//      .fit(data)
//
//    val scaleddata = scaler.transform(data).select("label", "scaledFeatures").toDF("label","features")
//
//    // 创建PCA模型，生成Transformer
//    val pca = new PCA()
//      .setInputCol("features")
//      .setOutputCol("pcaFeatures")
//      .setK(5)
//      .fit(scaleddata)
//
//    //  transform 数据，生成主成分特征
//    val pcaResult = pca.transform(scaleddata).select("label","pcaFeatures").toDF("label","features")
//
//    //  pcaResult.show(truncate=false)
//
//    // 将标签与主成分合成为一列
//    val assembler = new VectorAssembler()
//      .setInputCols(Array("label","features"))
//      .setOutputCol("assemble")
//    val output = assembler.transform(pcaResult)
//
//    // 输出csv格式的标签和主成分，便于可视化
//    val ass = output.select(output("assemble").cast("string"))
//    ass.write.mode("overwrite").csv("output.csv")
//
//    // 将经过主成分分析的数据，按比例划分为训练数据和测试数据
//    val Array(trainingData, testData) = pcaResult.randomSplit(Array(0.7, 0.3), seed = 20)
//
//    // 创建SVC分类器(Estimator)
//    val lsvc = new LinearSVC()
//      .setMaxIter(10)
//      .setRegParam(0.1)
//
//    // 创建pipeline, 将上述步骤连接起来
//    val pipeline = new Pipeline()
//      .setStages(Array(scaler, pca, lsvc))
//
//    // 使用串联好的模型在训练集上训练
//    val model = pipeline.fit(trainingData)
//
//    // 在测试集上测试
//    val predictions:DataFrame = model.transform(testData).select("prediction","label")
//    predictions.toDF().show()// 数据显示
//
//    // 计算精度
//    val evaluator = new MulticlassClassificationEvaluator()
//      .setLabelCol("label")
//      .setPredictionCol("prediction")
//      .setMetricName("accuracy")
//    val accuracy = evaluator.evaluate(predictions)
//
//    println(s"Accuracy = ${accuracy}")

//    session.stop()
  }

}
