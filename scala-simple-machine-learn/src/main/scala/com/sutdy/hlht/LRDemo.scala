package com.sutdy.hlht

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.SparkSession

/**
  * 逻辑回归的Demo
  */
object LRDemo {

  def main(args: Array[String]): Unit = {

    // 加载训练数据
    val session = SparkSession
      .builder()
      .master("local[3]")
      .appName("LRDemo")
      .getOrCreate()
    val training = session.read.format("libsvm").load("D:\\data\\spark\\spark-2.3.2-bin-hadoop2.7\\data\\mllib\\sample_libsvm_data.txt")

    val lr = new LogisticRegression()
      .setMaxIter(10)   // 设置最大迭代次数 默认100
      .setRegParam(0.3)  // 设置正则化系数  默认0 用于防止过拟合
      .setElasticNetParam(0.8)  // 正则化范式比（默认0），正则化有两种形式：L1（Lasso）和L2（Ridge），L1用于特征的稀疏化，L2用于防止过拟合

    // 训练模型
    val lrModel = lr.fit(training)

    // 打印coefficients和intercept
    println(s"每个特征对应系数Coefficients:${lrModel.coefficients} 截距Intercept: ${lrModel.intercept}")

    // 我们也可以使用多项式进行二元分类
    val mlr = new LogisticRegression()
      .setMaxIter(10) // 设置最大迭代次数 默认100
      .setRegParam(0.3) // 设置正则化系数 默认0 用于防止过拟合
      .setElasticNetParam(0.8) // 正则化范式比（默认0），正则化有两种形式:L1（Lasso）和L2（Ridge），L1用于特征的稀疏化，L2用于防止过拟合
      .setFamily("multinomial")

    val mlrModel = mlr.fit(training)

    println(s"Multinomial coefficients:${mlrModel.coefficientMatrix}")
    println(s"Multinomial coefficients:${mlrModel.interceptVector}")
  }

}
