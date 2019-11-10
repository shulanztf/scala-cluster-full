package com.api.stream.hlht.uoc

import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

class UocAuthserverLogPrint {
}

/**
  * 日志处理，标签日志
  *
  * @author 赵腾飞
  * @date 2019/11/9/009 18:06
  */
object UocAuthserverLogPrint {

  private val ZOOKEEPER_HOST = "localhost:2181"
  private val KAFKA_BROKER_CONSUMER = "localhost:31001"
  private val KAFKA_BROKER_PRODUCER = "localhost:31001"
  private val TRANSACTION_GROUP_ID = "test-consumer-group-flink"
  private val KAFKA_TOMCAT_TOPIC_NAME = "uoc-authserver-to-kafka"
  private val KAFKA_ELK_TOPIC_NAME = "uoc-kafka-to-elk"
  private val PREFIX_WORD = "UOCLOGPRINT"

  def main(args: Array[String]): Unit  = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE) // 仅执行一次语义

    val kafkaProps = new Properties()
    kafkaProps.setProperty("zookeeper.connect", ZOOKEEPER_HOST)
    kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER_CONSUMER)
    kafkaProps.setProperty("group.id", TRANSACTION_GROUP_ID)

    import org.apache.flink.api.scala._
    // 消费Kafka
    val transaction: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String](KAFKA_TOMCAT_TOPIC_NAME, new SimpleStringSchema(), kafkaProps))

    val textResult: DataStream[String] = transaction.filter(x=>{StringUtils.contains(x,PREFIX_WORD)}) // 只接收有[UOCLOGPRINT]的日志
      .map(x => {
      val json = new JSONObject()
      try {
        val tmpJson = JSON.parseObject(x);
        val text = tmpJson.getString("message")
        val arr = StringUtils.split(text, " ")
        json.put("message", text)
        json.put("tags", tmpJson.getString("tags"))
        json.put("request-time", arr(0) + " " + arr(1))
        json.put("client-id", arr(8))
        json.put("method", arr(9))
        json.put("url", arr(10))

        val map: Map[String, String] = getIp(arr(10))
        if (map.nonEmpty) {
          json.put("protocol", map.get("protocol").get)
          json.put("authority", map.get("authority").get)
          json.put("path", map.get("path").get)
        }
      } catch {
        case e: Exception => {
          json.put("errMsg", e.toString)
          e.printStackTrace()
        }
      }
      json.toJSONString
    })
    transaction.print("tomcat-source")
    textResult.print("kafka-sink")

    // sind到kafka
    val sink = new FlinkKafkaProducer[String](KAFKA_BROKER_PRODUCER, KAFKA_ELK_TOPIC_NAME, new SimpleStringSchema())
    textResult.addSink(sink)

    env.execute("uoc-auth-server-log-print")
  }

  /**
    * 解析http链接
    * @param url http接口url
    * @return
    */
  def getIp(url: String): Map[String, String] = {
    var map: Map[String, String] = Map()
    try {
      val url1 = new java.net.URL(url)
      map += ("protocol" -> url1.getProtocol)
      map += ("authority" -> url1.getAuthority)
      map += ("path" -> url1.getPath)
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
    map
  }

}
