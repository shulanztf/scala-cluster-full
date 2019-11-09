package com.sutdy.hlht

import java.util.Properties

import com.alibaba.fastjson.JSONObject
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

/**
  *  flink消费kafka的tomcat日志，清洗后，推送kafka
  * @author 赵腾飞
  * @date 2019/10/30/030 15:45
  */
object ReadingFromKafkaTomcat3 {

  private val ZOOKEEPER_HOST = "localhost:2181"
  private val KAFKA_BROKER = "localhost:9092"
  private val TRANSACTION_GROUP_ID = "test-consumer-group-flink"
  private val KAFKA_TOMCAT_TOPIC_NAME = "hmlc-tomcat-to-kafka-1"
  private val KAFKA_ELK_TOPIC_NAME = "hmlc-kafka-to-elk-1"
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE) // 仅执行一次语义

    val kafkaProps = new Properties()
    kafkaProps.setProperty("zookeeper.connect", ZOOKEEPER_HOST)
    kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER)
    kafkaProps.setProperty("group.id", TRANSACTION_GROUP_ID)

    import org.apache.flink.api.scala._
    // 消费Kafka
    val transaction:DataStream[String] = env.addSource(new FlinkKafkaConsumer[String](KAFKA_TOMCAT_TOPIC_NAME, new SimpleStringSchema(), kafkaProps))

    val textRslt:DataStream[String] = transaction.map(x=> {
      val json = new JSONObject()
      try {
        val arr = StringUtils.split(x," ")
        json.put("message",x)
        json.put("date",arr(0)+" "+arr(1))
        json.put("client_id",arr(8))
        json.put("method",arr(9))
        json.put("url",arr(10))

        val map:Map[String,String] = getIp(arr(10))
        if(map.nonEmpty) {
          json.put("protocol" , map.get("protocol").get)
          json.put("authority" , map.get("authority").get)
          json.put("path" , map.get("path").get)
        }
      }catch  {
        case e:Exception => {
          json.put("errMsg",e.toString)
          e.printStackTrace()
        }
      }
      json.toJSONString
    })
    transaction.print("tomcat-source")
    textRslt.print("kafka-sink")

    val sink = new FlinkKafkaProducer[String](KAFKA_BROKER,KAFKA_ELK_TOPIC_NAME,new SimpleStringSchema())
    textRslt.addSink(sink)

    env.execute("flink-tomcat-kafka-3")
  }

  def getIp(url:String): Map[String,String] = {
    var map:Map[String,String] = Map()
    try {
      val url1 = new java.net.URL(url)
      map += ("protocol" -> url1.getProtocol)
      map += ("authority" -> url1.getAuthority)
      map += ("path" -> url1.getPath)
    }catch  {
      case ex:Exception => {
        ex.printStackTrace()
      }
    }
    map
  }

}
