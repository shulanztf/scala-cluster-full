package com.msb.util

import java.io.InputStream
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * 工具类
  */
object SparkSessionBase {

  /**
    * 创建SQL式SparkSession
    */
  def createSparkSession(): SparkSession = {
    val inputStream: InputStream = SparkSessionBase.getClass.getClassLoader.getResourceAsStream("spark-conf.properties") //加载配置文件
    val prop = new Properties()
    prop.load(inputStream)
    val conf = new SparkConf()
    conf.setAppName(prop.getProperty("spark.app.name"))
    conf.setMaster("local")
    conf.set("hive.metastore.uris", prop.getProperty("hive.metastore.uris"))

    val enableHive = prop.getProperty("enable.hive.support", "false").toBoolean
    if (enableHive) {
      SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    } else {
      SparkSession.builder().config(conf).getOrCreate()
    }
  }

  /**
    * 创建流式StreamingContext
    *
    * @return
    */
  def createStreamingContext() = {
    val inputStream: InputStream = SparkSessionBase.getClass.getClassLoader.getResourceAsStream("spark-conf.properties") //加载配置文件
    val prop = new Properties()
    prop.load(inputStream)
    val sparkConf = new SparkConf()
    sparkConf.setAppName(prop.getProperty("spark.streaming.app.name"))
    sparkConf.setMaster("local[3]")
    sparkConf.set("hive.metastore.uris", prop.getProperty("hive.metastore.uris"))
    new StreamingContext(sparkConf, Durations.seconds(1)) //流窗口间隔1秒
  }

}
