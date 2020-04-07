package com.msb.recall

import com.msb.util.{HBaseUtil, PropertiesUtils, SparkSessionBase}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer


/**
  * ALS推荐算法(交替最小二乘法)
  */
object ALSRecall {
"""
1、user_profile表
2、recall表
3、history_recall表
"""

  case class item(userID: Int, itemID: Int, score: Float)

  def main(args: Array[String]): Unit = {
    val session = SparkSessionBase.createSparkSession()
    val table = PropertiesUtils.getProp("user.profile.hbase.table")
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", PropertiesUtils.getProp("hbase.zookeeper.property.clientPort"))
    conf.set("hbase.zookeeper.quorum", PropertiesUtils.getProp("hbase.zookeeper.quorum"))
    conf.set("zookeeper.znode.parent", PropertiesUtils.getProp("zookeeper.znode.parent"))
    conf.set(TableInputFormat.INPUT_TABLE, table)

    val hbaseRdd: RDD[(ImmutableBytesWritable, Result)] = session.sparkContext.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    import session.implicits._

    //读取hbase信息,扫描信息
    val trainRDD:RDD[((String,(Int,Double)),Long)] = hbaseRdd.flatMap(data => {
      val list = new ListBuffer[(String,(Int, Double))]()
      val result = data._2
      for(rowKv <- result.rawCells()) {
        val userID:String = new String(rowKv.getRowArray, rowKv.getRowOffset, rowKv.getRowLength, "UTF-8")
        val colName = new String(rowKv.getQualifierArray, rowKv.getQualifierOffset, rowKv.getQualifierLength, "UTF-8")
        if(colName.contains("itemID")) {
          val itemID = colName.split(":")(1).toInt
          val value = new String(rowKv.getValueArray, rowKv.getValueOffset, rowKv.getValueLength, "UTF-8")
          val score = value.split("\\|")(1).split(":")(1).toDouble
          list.+=((userID,(itemID.toInt,score)))
        }
      }
      list.iterator
    }).zipWithUniqueId()//数据末尾，生成全局序号，非连续

    val fictIndex2itemIDMap = trainRDD.map(data => {
      (data._2.toInt,data._1._1)//全局序号，用户id
    }).collect() //数据拉回Driver端，数据量大时，不要做
      .toMap
    val fictIndex2itemIDMapBroad = session.sparkContext.broadcast(fictIndex2itemIDMap)//广播变量

    val trainDF:DataFrame = trainRDD.map(data => {
      val userID = data._1
      val itemID = data._1._2._1.toInt
      val score = data._1._2._2.toFloat
      val fictIndex = data._2.toInt//全局序号
      item(fictIndex,itemID,score)
    }).toDF()

    val als = new ALS() //ALS推荐模型
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userID")
      .setItemCol("itemID")
      .setRatingCol("score")

    val model:ALSModel = als.fit(trainDF) //预测推荐
    trainDF.show(10) //数据显示

    val recommondRest = model.recommendForAllUsers(10)//向所有用户，推荐前10个商品
      .withColumn("itemID",explode(col("recommendations")))//针对itemID，拆分recommendations列，行转列处理
      .drop("recommendations")//删除recommendations列
      .select(col("userID"),col("itemID").getField("itemID").alias("itemID"))
    recommondRest.printSchema()
    recommondRest.show(10)//数据显示

    /**
      * create 'recall', {NAME=>'als', TTL=>2592000, VERSIONS=>999999}
      * alter 'recall', {NAME=>'content', TTL=>2592000, VERSIONS=>999999}
      * alter 'recall', {NAME=>'online', TTL=>2592000, VERSIONS=>999999}
      *
      * Spark  scala.reflect.api.TypeTags$PredefTypeCreat版本问题
      */
    recommondRest.rdd.map(row => {
      val userID = fictIndex2itemIDMapBroad.value.get(row.getAs[Int]("userID")).get//获取广播变量中的用户id
      val itemID = row.getAs[Int]("itemID")
      (userID,itemID)
    }).groupByKey().foreachPartition(partition => {
      val tableName = PropertiesUtils.getProp("user.recall.hbase.table")
      val hisTableName = PropertiesUtils.getProp("user.history.recall.hbase.table")
      val conf = HBaseUtil.getHBaseConfiguration()
      val conn = ConnectionFactory.createConnection(conf)
      val htable = HBaseUtil.getTable(conf,tableName)
      val histable = HBaseUtil.getTable(conf,hisTableName)
      for(elem <- partition) {
        /**
          * 在推荐过程中，如果已经推荐过的商品，就不能再推荐,会从召回表中删除
          * 那么再过一段时间后，会重新计算出召回结果，
          * 此时的召回结果需要和历史表中的数据计算交集，防止重复推荐
          */
        val userID = elem._1
        val hisRecalls = HBaseUtil.getRecord(hisTableName,userID,conn).map(_.toInt).toSet//历史数据
        val itemIDs = elem._2.map(_.toInt).toSet
        val diff = itemIDs -- hisRecalls//过滤重复数据
        if(diff.size > 0) {
          val recall = diff.mkString("|")//拆分字段值
          //添加找到recall,hbase表
          val put = new Put(Bytes.toBytes(userID))
          put.addColumn(Bytes.toBytes("als"), Bytes.toBytes("item"), Bytes.toBytes(recall))
          htable.put(put)
          //添加到历史recall表,hbase表
          val hput = new Put(Bytes.toBytes(userID))
          hput.addColumn(Bytes.toBytes("recommond"), Bytes.toBytes("recommond"), Bytes.toBytes(recall))
          histable.put(hput)
        }
      }
      conn.close()
      htable.close()
      histable.close()
    })
    session.close()
  }

}
