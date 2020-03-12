package com.msb.util

import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable


object HBaseUtil {
  def getHBaseConfiguration(): Configuration = {
    val prop = new Properties()
    val inputStream = SparkSessionBase.getClass.getClassLoader.getResourceAsStream("spark-conf.properties")
    prop.load(inputStream)

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", prop.getProperty("hbase.zookeeper.quorum"))
    conf.set("hbase.zookeeper.property.clientPort", prop.getProperty("hbase.zookeeper.property.clientPort"))
    conf
  }

  /**
    * 返回HTable
    *
    * @param tableName
    * @return
    */
  def getTable(conf: Configuration, tableName: String): HTable = {
    //    val conf = HBaseUtil.getHBaseConfiguration()
    //    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    new HTable(conf, tableName)
  }
}
