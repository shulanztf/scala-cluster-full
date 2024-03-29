package com.sutdy.hlht

import java.util.Properties

import com.alibaba.fastjson.JSONObject
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.shaded.guava18.com.google.common.base.Throwables
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.slf4j.{Logger, LoggerFactory}

/**
  * @author 赵腾飞
  * @date 2019/11/1/001 17:37
  */
object ReadingFromKafkaTomcat4 {
  val logger: Logger = LoggerFactory.getLogger(ReadingFromKafkaTomcat4.getClass)

  private val ZOOKEEPER_HOST = "192.168.174.105:2181"
  private val KAFKA_BROKER = "192.168.174.105:31001"
  private val TRANSACTION_GROUP_ID = "test-consumer-group-flink"
  private val KAFKA_TOMCAT_TOPIC_NAME = "hmlc-tomcat-to-kafka-4"
  private val KAFKA_ELK_TOPIC_NAME = "hmlc-kafka-to-elk-4"

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
    val transaction: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String](KAFKA_TOMCAT_TOPIC_NAME, new SimpleStringSchema(), kafkaProps))

    val textResult: DataStream[String] = transaction.map(x => {
      val json = new JSONObject()
      try {
        val arr = StringUtils.split(x, " ")
        json.put("message", x)
        json.put("date", arr(0) + " " + arr(1))
        json.put("client_id", arr(8))
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
          logger.error("get ip error:{}", Throwables.getStackTraceAsString(e))
        }
      }
      json.toJSONString
    })
    transaction.print("tomcat-source")
    textResult.print("kafka-sink")

    // sind到kafka
    val sink = new FlinkKafkaProducer[String](KAFKA_BROKER, KAFKA_ELK_TOPIC_NAME, new SimpleStringSchema())
    textResult.addSink(sink)

    env.execute("flink-tomcat-kafka-4")
  }

  def getIp(url: String): Map[String, String] = {
    var map: Map[String, String] = Map()
    try {
      val url1 = new java.net.URL(url)
      map += ("protocol" -> url1.getProtocol)
      map += ("authority" -> url1.getAuthority)
      map += ("path" -> url1.getPath)
    } catch {
      case ex: Exception => {
        logger.error("解析http url error:{}", Throwables.getStackTraceAsString(ex))
      }
    }
    map
  }


}
