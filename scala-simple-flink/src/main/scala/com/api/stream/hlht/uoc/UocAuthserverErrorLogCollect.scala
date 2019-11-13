package com.api.stream.hlht.uoc

/**
  * @ClassName: UocAuthserverErrorLogCollect
  * @Author: zhaotf
  * @Description: authserver异常日志处理
  * @Date: 2019/11/13 0013 
  */
class UocAuthserverErrorLogCollect {

}


object UocAuthserverErrorLogCollect {
  private val ZOOKEEPER_HOST = "localhost:2181"
  private val KAFKA_BROKER_CONSUMER = "localhost:31001"
  private val KAFKA_BROKER_PRODUCER = "localhost:31001"
  private val TRANSACTION_GROUP_ID = "test-consumer-group-flink"
  private val KAFKA_TOMCAT_TOPIC_NAME = "uoc-authserver-to-kafka"
  private val KAFKA_ELK_TOPIC_NAME = "uoc-kafka-to-elk"
  private val PREFIX_WORD = "UOCLOGPRINT"

  def main(args: Array[String]): Unit = {

  }

}
