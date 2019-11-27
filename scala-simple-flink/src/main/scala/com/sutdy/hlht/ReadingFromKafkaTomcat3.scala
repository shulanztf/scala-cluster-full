package com.sutdy.hlht

import java.util.Properties

import com.alibaba.fastjson.JSONObject
import com.api.stream.hlht.uoc.UocAuthserverLogPrint.logger
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.guava18.com.google.common.base.Throwables
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.slf4j.{Logger, LoggerFactory}

/**
  *  flink消费kafka的tomcat日志，清洗后，推送kafka
  * @author 赵腾飞
  * @date 2019/10/30/030 15:45
  */
object ReadingFromKafkaTomcat3 {
  val logger: Logger = LoggerFactory.getLogger(ReadingFromKafkaTomcat3.getClass)

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
          logger.error("get ip error:{}", Throwables.getStackTraceAsString(e))
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
        logger.error("解析http url error:{}", Throwables.getStackTraceAsString(ex))
      }
    }
    map
  }

}



/**
  * Flink的累加器使用
  */
object CounterBatch {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val text = env.fromElements("Hello Jason What are you doing Hello world")
    val counts = text
      .flatMap(_.toLowerCase.split(" "))
      .map(new RichMapFunction[String, String] {
        //创建累加器
        val acc = new IntCounter()
        override def open(parameters: Configuration): Unit = {
          super.open(parameters)
          //注册累加器
          getRuntimeContext.addAccumulator("accumulator", acc)
        }
        override def map(in: String): String = {
          //使用累加器
          this.acc.add(1)
          in
        }
      }).map((_,1))
      .groupBy(0)
      .sum(1)
    counts.writeAsText("/data/flink/conunter-test.txt/").setParallelism(1)
    val res = env.execute("Accumulator Test")
    //获取累加器的结果
    val num = res.getAccumulatorResult[Int]("accumulator")
    println(num)
  }
}