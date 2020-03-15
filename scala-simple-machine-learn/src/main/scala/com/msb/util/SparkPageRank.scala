package com.msb.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  *
  */
object SparkPageRank {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("SparkPageRank")
      .master("local")
      .getOrCreate()

    //迭代几次
    val iters = if (args.length > 1) args(1).toInt else 10

    //KV格式
    val lines:RDD[(String,String)] = spark.sparkContext.parallelize(List(
      //A指向B
      ("A", "B"),
      ("A", "C"),
      ("B", "A"),
      ("B", "C"),
      ("C", "A"),
      ("C", "B"),
      ("C", "D"),
      ("D", "C")
    ))
    /**
      * A [B,C]
      * B [A,C]
      * ...
      */
    val links = lines.groupByKey().cache()
    //将value 全部置为 1  初始PR：1
    /**
      * A 1
      * B 1
      * ...
      */
    var ranks:RDD[(String,Double)] = links.mapValues(v => 1.0)//遍历value，设置初始权重

    for (i <- 1 to iters) {
      val contribs = links.join(ranks).values.flatMap { case (urls, rank) =>
        val size = urls.size //出链数量
        urls.map(url => (url, rank / size)) // rank / size 输出的能量
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _) //更新权重
    }

    val output: Array[(String, Double)] = ranks.collect()
    output.foreach(tup => println(s"${tup._1} has rank:  ${tup._2}"))
    spark.stop()
  }

}
