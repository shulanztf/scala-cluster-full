package com.sutdy.hlht.dataLabel

import java.util.Calendar

import com.msb.util.DataUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * 积分数据特征化,用此
  */
object PointsOrderFeatures {
  /**
    * 数据格式：
    * 自增id 用户id 发生时间
    * 目标数据格式：
    * 自增id 时间窗口内数据量 发生时间段
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val conf:SparkConf = new SparkConf()
    conf.setMaster("local")
    val session:SparkSession = SparkSession.builder().config(conf).appName("PointsDataFeatures").getOrCreate()
    import session.implicits._
    val dataRDD:RDD[String] = session.sparkContext.textFile("D:\\data\\hlht\\point\\points-data-label\\points-order-20200316.txt")

    val valueDF1:DataFrame = dataRDD.map(line => {
      val array = StringUtils.split(line, "\t")
      //    1、统计时间窗口内数据量
      //    2、发生时间段离散化
      val cal = Calendar.getInstance()
      cal.setTime(DataUtils.parseDate(array(2), DataUtils.PATTERN(7))) // 转换订单发生时间，转为日期对角
      val timeInterval = cal.get(Calendar.HOUR_OF_DAY)
      (array(0), array(1), timeInterval,DataUtils.getDateLong(array(2), DataUtils.PATTERN(7)))
    }).toDF("id","memberId","time","times")
//    valueDF1.show(20)//  数据显示

    // 注册成表
    valueDF1.createOrReplaceTempView("pointsOrder")
    valueDF1.createOrReplaceTempView("pointsOrderTmp")
    val sqlResult3:DataFrame = session.sql("select t1.*, t2.id as tid,t2.times as tmptimes,t2.memberId as tmpMemberId " +
      " from pointsOrder t1 " +
      " left join pointsOrderTmp t2 on t1.memberId = t2.memberId " +
      "    and t1.times >= t2.times " +
      "    and t1.times < t2.times + 10000 ")
//    sqlResult3.show(20)//  数据显示

    val valueDF3:DataFrame = sqlResult3.rdd.filter(row => {
      StringUtils.equals(row.getAs[String]("memberId"),row.getAs[String]("tmpMemberId"))
    }).map(row =>{
      ((row.getAs[String]("id"),row.getAs[Int]("time"),row.getAs[String]("memberId"),row.getAs[String]("tmpMemberId")),row.getAs[String]("tid"))
    }).groupByKey().map(row => {
      (row._1._1,row._1._2,row._2.size)
    }).toDF()
//    valueDF3.show(10000)//  数据显示

//      //    3、数据输出
//    valueDF3.coalesce(1).write.mode(SaveMode.Overwrite).parquet("/data/msb/RecommenderProgram/models/PointsOrderFeatures.parquet")//数据存储本地


    session.close() // 资源关闭
    println("end......")
  }
}


//object Test1 {
//  def main(args: Array[String]): Unit = {
////    val cal = Calendar.getInstance()
//    val date = DataUtils.getDateLong("2020-03-16 12:26:58", DataUtils.PATTERN(7))
//    println("ddd:",date)
//    //    val timeInterval = cal.get(Calendar.HOUR_OF_DAY)
//    //    println("abc",timeInterval)
//  }
//}