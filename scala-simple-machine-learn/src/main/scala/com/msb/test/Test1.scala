package com.msb.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * repartition改变分区数验证
  */
object Test1 {

  def main(args: Array[String]): Unit = {
    println("111:",this.getClass.getSimpleName)
    val session = SparkSession.builder.appName(this.getClass.getSimpleName).master("local").getOrCreate()
    import session.implicits._
//    val conf = new SparkConf().setAppName("DecisionTreeDemo").setMaster("local[4]")
//    val sc = new SparkContext(conf)
    val data = session.sparkContext.textFile("D:\\data\\spark\\text\\data-1.txt")
    data.collect
    println("abc:",data.partitions.size)
//    data.foreach(println)

    val rdd2 = data.mapPartitionsWithIndex(func1)
    println("edf:", rdd2.partitions.size)
    rdd2.toDF().show(100) //数据显示
//    rdd2.mapPartitionsWithIndex()

    val rdd3 = data.repartition(3).mapPartitionsWithIndex(func1)
    println("ghk:", rdd3.partitions.size)
    rdd3.toDF().show(100)

    session.close()
  }

  /**
    * mapPartitionsWithIndex算子用
    * @param index 分区号
    * @param iter 数据
    * @return
    */
  def func1(index:Int,iter:Iterator[String]) : Iterator[String] = {
    iter.toList.map(x => "[partID:"+index+",value:"+x+"]").iterator
  }

}

//执行日志
/*
(111:,Test1$)
(abc:,1)
(edf:,1)
+--------------------+
|               value|
+--------------------+
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
+--------------------+

(ghk:,3)
+--------------------+
|               value|
+--------------------+
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:0,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:1,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
|[partID:2,value:3...|
+--------------------+


Process finished with exit code 0


  */