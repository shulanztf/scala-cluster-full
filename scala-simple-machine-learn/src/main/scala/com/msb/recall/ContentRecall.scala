package com.msb.recall

import com.msb.util.{HBaseUtil, PropertiesUtils, SparkSessionBase}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/**
  * 内容召回
  */
object ContentRecall {
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
    val session = SparkSessionBase.createSparkSession()
    val df = session.sql("SELECT a.sn, a.item_id,a.duration,b.length " +
      "FROM program.user_action a " +
      "JOIN program.item_info b ON a.item_id = b.item_id where a.sn != 'unknown' ")

    val itemID2userID:RDD[(Int,String)]  = df.rdd.flatMap(row => {
      val list = new ListBuffer[(Int,String)]()
      val userID = row.getAs[String]("sn")
      val itemID = row.getAs[Int]("item_id")
      val duration = row.getAs[Long]("duration")
      val length = row.getAs[Long]("length")

      if(duration < length) {
        val scalaDuration = (duration*1.0) /length
        if(scalaDuration > 0.1) {
          list.+=((itemID,userID))
        }
      }
      list.iterator
      // 用户可能会点击这个节目N多次，那么在计算内容召回的时候，应该去重，不然内容召回表中会有大量重复数据
    }).distinct()

    val table = PropertiesUtils.getProp("similar.hbase.table")
    val conf = HBaseUtil.getConf(table)

    var hbaseRdd:RDD[(ImmutableBytesWritable,Result)] = session.sparkContext.newAPIHadoopRDD(conf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
    var similarPro:RDD[(Int,Int)] = hbaseRdd.flatMap(data => {
      val list = new ListBuffer[(Int,Int)]()
      val result = data._2
      for(rowKv <- result.rawCells()) {
        val rowkey = new String(rowKv.getRowArray,rowKv.getRowOffset,rowKv.getRowLength,"UTF-8")
        val colName = new String(rowKv.getQualifierArray,rowKv.getQualifierOffset,rowKv.getQualifierLength,"UTF-8")
        list.+=((rowkey.toInt,colName.toInt))
      }
       list.iterator
    })

    itemID2userID.join(similarPro).map(x => {
      (x._2._1,x._2._2)
    }).groupByKey().foreachPartition(partition => {
      val tableName = PropertiesUtils.getProp("user.recall.hbase.table")
      val hisTableName = PropertiesUtils.getProp("user.history.recall.hbase.table")
      val conf = HBaseUtil.getHBaseConfiguration()
      val conn = ConnectionFactory.createConnection(conf)
      val htable = HBaseUtil.getTable(conf,tableName)
      val histable = HBaseUtil.getTable(conf,hisTableName)
      for(elem <- partition) {
        val userID = elem._1
        val hisRecalls = HBaseUtil.getRecord(hisTableName,userID,conn).map(_.toInt).toSet
        val itemIDs = elem._2.toSet
        val diff = itemIDs -- hisRecalls

        if(diff.size > 0) {
          val recall = diff.mkString("|")
          //添加找到recall
          val put = new Put(Bytes.toBytes(userID))
          put.addColumn(Bytes.toBytes("content"),Bytes.toBytes("item"),Bytes.toBytes(recall))
          htable.put(put)
          // 添加到历史recall表
          val hput = new Put(Bytes.toBytes(userID))
          hput.addColumn(Bytes.toBytes("recommond"),Bytes.toBytes("recommond"),Bytes.toBytes(recall))
          histable.put(hput)
        }
      }
      conn.close()
      htable.close()
      histable.close()
    })
    df.show()
  }

}























