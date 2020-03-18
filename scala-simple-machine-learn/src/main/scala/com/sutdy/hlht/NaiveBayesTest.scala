package com.sutdy.hlht

import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

/**
  * https://msd.misuland.com/pd/2884250137616453172 朴素贝叶斯算法+Spark MLlib代码Demo
  */
object NaiveBayesTest {


  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("NaiveBayesTest").setMaster("local")
    val sc=new SparkContext(conf)

    //处理数据
    val data = sc.textFile("D:\\data\\spark\\data\\sample_naive_bayes_data.txt")
    val parsedData = data.map(line => {
      val parts = line.split(",")
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(" ").map(_.toDouble)))
    })

    //数据分为训练数据(60%)，测试数据(40%)
    val splits = parsedData.randomSplit(Array(0.6,0.4),seed = 11L)
    val trainingData = splits(0)     //训练数据
    val testingData = splits(1)      //测试数据

    //训练朴素贝叶斯模型。
    //参数lambda：平滑因子，防止条件概率等于0；modelType：模型类型，spark支持的有multinomial(多项式，默认)和bernoulli(伯努利)两种。
    val model = NaiveBayes.train(trainingData,lambda = 1,modelType = "multinomial")

    //输入测试数据
    val predictionAndLabel = testingData.map(p=>(model.predict(p.features),p.label))
    //计算准确度：预测错误的测试数据数目/全部测试数据
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / testingData.count()

    println("result:")
    println("training.count:" + trainingData.count())
    println("test.count:" + testingData.count())
    println("model.modelType:" + model.modelType)
    println("accuracy:" + accuracy)
    println("prediction"+"\t"+"label")
    val prin = predictionAndLabel.take(10)
    for (i <- 0 to prin.length-1){
      println(prin(i)._1+"\t"+"\t"+"\t"+prin(i)._2)
    }

//    //输入测试瓜向量，并为其分类
//    val testV:Vector = Vectors.dense(Array[Double](0,1,2,0,1,0))
//    val result = model.predict(testV)
//    if(result==0){
//      println("测试瓜是个好瓜")
//    }else{
//      println("测试瓜不是个好瓜")
//    }
  }

}
