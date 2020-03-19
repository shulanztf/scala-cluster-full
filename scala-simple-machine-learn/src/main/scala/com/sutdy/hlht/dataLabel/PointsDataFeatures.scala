package com.sutdy.hlht.dataLabel

import java.util.{Calendar, Date}

import com.msb.util.DataUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 积分数据特征化
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
    val dataRDD:RDD[String] = session.sparkContext.textFile("D:\\data\\hlht\\point\\points-data-label\\points-data-full-20200318.txt")

    val valueRDD:RDD[(String,Int,Int)] = dataRDD.map(line => {
      val array = StringUtils.split(line, "\t")
      ((array(0), array(1)), array(2))
    }).groupByKey().map(row => {
      //    1、统计时间窗口内数据量
      //    2、发生时间段离散化
      val cal = Calendar.getInstance()
      cal.setTime(DataUtils.parseDate(row._1._2, DataUtils.PATTERN(7))) //TODO，转换订单发生时间，转为日期对角
      val timeInterval = cal.get(Calendar.HOUR_OF_DAY)
      (row._1._1, timeInterval, row._2.size)
    })
    valueRDD.toDF("id","times","size").show(20000)//  数据显示
//    valueRDD.toDF().show(10)//  数据显示

//    //    3、数据输出
//    valueRDD.toDF().coalesce(1).write.mode(SaveMode.Overwrite).parquet("/data/msb/RecommenderProgram/models/PointsDataFeatures.parquet")//数据存储本地

    session.close() // 资源关闭
    println("end......")
  }

}

