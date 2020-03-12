package com.msb.util

import java.util.Properties


/**
  * 配置文件读取工具类
  */
object PropertiesUtils {
  val prop = new Properties()
  val inputStream = PropertiesUtils.getClass.getClassLoader.getResourceAsStream("spark-conf.properties")
  prop.load(inputStream)

  def getProp(name: String): String = {
    prop.getProperty(name)
  }

  def main(args: Array[String]): Unit = {
    println(PropertiesUtils.getProp("spark.streaming.app.name"))
  }
}
