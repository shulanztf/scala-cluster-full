package com.msb.recall

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 内容召回
  */
object ContentRecallLocal {
  """
1、user_action表数据文件：D:\resource\机器学习\推荐系统06-Flume实时采集数据及提取关键词20190803_tmp.csv
示例：cd38023850,124078,685,2019-08-03 18:56:38,873
sn STRING comment "sn",
item_id INT comment "item_id",
duration BIGINT comment "duration",
time String comment "time",
position BIGINT comment "position"
2、item_info表数据文件：D:\data\msb\RecommenderProgram\item_info-10.txt
示例：157781	2012-12-18 12:40:07.0	1991-07-01T00:00:00	一条狗的使命2	Miguel Bose - Como Un Lobo/Miguel Bose - Como Un Lobo		Miguel Bose		216	music	欧美	英语	2	false
id BIGINT comment "item unique key",
create_date String comment "create date",
air_date String comment "air_date",
title String comment "title",
name String comment "name",
desc String comment "desc",
keywords String comment "keywords",
focus String comment "focus",
length BIGINT comment "length",
content_model String comment "content_model",
area String comment "area",
language String comment "language",
quality String comment "quality",
is_3d String comment "is_3d"
3、hbase.table = history_recall表
userID,
4、hbase.table = recall表


"""


  def main(args: Array[String]): Unit = {
    val conf:SparkConf = new SparkConf()
    conf.setMaster("local")
    val session:SparkSession = SparkSession.builder().config(conf).appName(this.getClass.getSimpleName).getOrCreate()
    import session.implicits._

    val userActionRDD:DataFrame = session.sparkContext.textFile("D:\\data\\msb\\RecommenderProgram\\user_action-1000.csv")
      .map(line => {
        val arr = StringUtils.splitPreserveAllTokens(line,",")
        (arr(0),arr(1),arr(2),arr(3),arr(4))
      }).toDF("sn","item_id","duration","time","position")
    val itemInfoRDD:DataFrame = session.sparkContext.textFile("D:\\data\\msb\\RecommenderProgram\\item_info-10.txt")
        .map(line =>{
          val arr = StringUtils.splitPreserveAllTokens(line,"\t")//拆分时，保留空字段位
          (arr(0),arr(1),arr(2),arr(3),arr(4),arr(5),arr(6),arr(7),arr(8),arr(9),arr(10),arr(11),arr(12))
        }).toDF("id","create_date","air_date","title","name","desc","keywords","focus","length","content_model","area","language","quality")
    userActionRDD.createOrReplaceTempView("user_action")
    itemInfoRDD.createOrReplaceTempView("item_info")
    val df:DataFrame = session.sql("SELECT a.sn, a.item_id,a.duration,b.length " +
      "FROM user_action a " +
      "JOIN item_info b ON a.item_id = b.id where a.sn != 'unknown' ")
    df.show(10,truncate = false)// 数据显示，不折行

    session.close()
  }

}


object test11 {

  def main(args: Array[String]): Unit = {
    val text = "391118\t2014-02-14 17:47:34.0\t2014-02-14T00:00:00\tHeyPop国际音乐流行榜 2013/10/23\tHeyPop国际音乐流行榜 2013/10/23/HeyPop国际音乐流行榜 2013/10/23\t20131023HeyPop国际音乐榜 黄喜主持[小星星+纸牌屋+Ooh La La+我的宣言]\t\t\t301\tmusic\t港台\t粤语\t2\tfalse"
    val arr = StringUtils.splitPreserveAllTokens(text,"\t")
//    println(arr.length)
    println((arr(0),arr(1),arr(2),arr(3),arr(4),arr(5),arr(6),arr(7),arr(8),arr(9),arr(10),arr(11),arr(12)))
  }
}