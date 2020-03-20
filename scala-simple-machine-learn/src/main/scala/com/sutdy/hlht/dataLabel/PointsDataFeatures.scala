package com.sutdy.hlht.dataLabel

import java.util.{Calendar, Date}

import com.msb.util.DataUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * 积分数据特征化,KMeans数据生成
  */
object PointsDataFeatures {

  /**
    * 数据格式：
    * 自增id 发生时间 时间窗口内该用户其它数据id 对应的发生时间
    * 目标数据格式：
    * 自增id 时间窗口内数据量 发生时间段
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val conf:SparkConf = new SparkConf()
    conf.setMaster("local")
    val session:SparkSession = SparkSession.builder().config(conf).appName("PointsDataFeatures").getOrCreate()
    import session.implicits._
    val dataRDD:RDD[String] = session.sparkContext.textFile("D:\\data\\hlht\\point\\points-data-label\\points-label-train-20200319.txt")

    val valueRDD:DataFrame = dataRDD.map(line => {
      val array = StringUtils.split(line, "\t")
//      (array(0), "1:"+array(1), "2:"+array(2))
      array(0)+" 1:"+array(2)+" 2:"+array(3)
    }).toDF()
    valueRDD.show(10000)//  数据显示
//    valueRDD.toDF().show(10)//  数据显示

//    //    3、数据输出
//    valueRDD.toDF().coalesce(1).write.mode(SaveMode.Overwrite).parquet("/data/msb/RecommenderProgram/models/PointsDataFeatures.parquet")//数据存储本地

    session.close() // 资源关闭
    println("end......")
  }

}

