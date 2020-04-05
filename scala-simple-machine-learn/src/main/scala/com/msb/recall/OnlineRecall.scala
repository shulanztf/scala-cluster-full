package com.msb.recall

import java.util.Properties

import com.msb.util.{HBaseUtil, RedisUtil, SparkSessionBase}
import org.apache.hadoop.hbase.client.{Connection, HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Durations, StreamingContext}

import scala.collection.immutable


/**
  *
  */
object OnlineRecall {

  def main(args: Array[String]): Unit = {
    val prop = new Properties()
    val inputStream = SparkSessionBase.getClass.getClassLoader.getResourceAsStream("spark-conf.properties")
    prop.load(inputStream)

    val session = SparkSessionBase.createSparkSession()
    val sc = session.sparkContext
    val streamingContext = new StreamingContext(sc,Durations.seconds(3))
    val bootstrapServers = prop.getProperty("bootstrap.servers")
    val groupId = prop.getProperty("group.id")
    val topicName = prop.getProperty("topic.name")
    val maxPoll = prop.getProperty("max.poll")
    val redisHost = prop.getProperty("redis.host")
    val redisPort = prop.getProperty("redis.port")
    val dbIndex = prop.getProperty("redis.hot.db")

    val kafkaParams = Map(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> maxPoll.toString,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    val kafkaTopicDS:InputDStream[ConsumerRecord[String,String]] = KafkaUtils.createDirectStream(streamingContext,LocationStrategies.PreferBrokers,
      ConsumerStrategies.Subscribe[String, String](Set(topicName), kafkaParams))

    val itemInfo = session.table("program.item_info")
    //获取每一个节目的总时长
    val itemID2LengthMap:immutable.Map[Int,Long] = itemInfo.rdd.map(row => {
      val itemID = row.getAs[Int]("item_id")
      val length = row.getAs[Long]("length")
      (itemID,length)
    }).collect() // 拉回driver端
      .toMap
    val itemID2LengthMapBroad = streamingContext.sparkContext.broadcast(itemID2LengthMap)//广播变量

    // 遍历处理kafka数据
    kafkaTopicDS.foreachRDD(rdd => {
      rdd.foreachPartition(row => {
        val itemID2LengthMap = itemID2LengthMapBroad.value //获取广播变量
        val conn = HBaseUtil.getConn("program_similar")
        val htable = HBaseUtil.getTable(conn.getConfiguration, "recall")
        val histable = HBaseUtil.getTable(conn.getConfiguration,"history_recall")

        for(elem <- row) {
          val value = elem.value()
          val elems = value.split(",")
          val userID = elems(0)
          val itemID = elems(1)
          val duration = elems(2)
          val similars:Set[Int] = HBaseUtil.getRecord3("program_similar", itemID, conn).map(_.toInt).toSet
          val hisRecalls:Set[Int] = HBaseUtil.getRecord("history_recall", userID, conn).map(_.toInt).toSet
          val diff:Set[Int] = similars -- hisRecalls //过滤历史已推送数据
          val score = duration.toInt/(itemID2LengthMap.get(itemID.toInt).get * 1.0) * 10
          if(diff.size > 0 && score > 5) {
            //将召回的相似节目写入在HBase中
            saveAsHBase(elem, htable, histable, conn)
          }
          println("itemID2LengthMap.get(itemID.toInt).get" + itemID2LengthMap.get(itemID.toInt).get)
          // 节目热度递增计算
          if(score > 5) {
            println("itemID2LengthMap.get(itemID.toInt).get" + itemID2LengthMap.get(itemID.toInt).get)
            //将节目的热度+1
            RedisUtil.init(redisHost, redisPort.toInt)
            RedisUtil.updateHot(1, "hot", itemID)
          }
        }
        htable.close()
        histable.close()
        conn.close()
      })
    })
    streamingContext.start()
    streamingContext.awaitTermination()
    streamingContext.stop()
  }


  def saveAsHBase(elem: ConsumerRecord[String, String], htable: HTable, histable: HTable, conn: Connection): Unit = {
    val value = elem.value()
    val elems = value.split(",")
    val userID = elems(0)
    val itemID = elems(1)
    val similars = HBaseUtil.getRecord3("program_similar", itemID, conn).map(_.toInt).toSet
    val hisRecalls = HBaseUtil.getRecord("history_recall", userID, conn).map(_.toInt).toSet
    val diff = similars -- hisRecalls
    if (diff.size > 0) {
      val recall = diff.mkString("|")
      //添加找到recall
      val put = new Put(Bytes.toBytes(userID))
      put.addColumn(Bytes.toBytes("online"), Bytes.toBytes("item"), Bytes.toBytes(recall))
      htable.put(put)
      //添加到历史recall表
      val hput = new Put(Bytes.toBytes(userID))
      hput.addColumn(Bytes.toBytes("recommond"), Bytes.toBytes("recommond"), Bytes.toBytes(recall))
      histable.put(hput)
    }
  }
}



























