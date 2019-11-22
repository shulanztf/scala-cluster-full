package com.sutdy.hlht

import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.math.NumberUtils
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.shaded.guava18.com.google.common.base.Throwables
import org.slf4j.{Logger, LoggerFactory}

/**
  *
  * @ClassName: RedisDataAnalysis
  * @Author: zhaotf
  * @Description: redis数据分析
  * @Date: 2019/11/22 0022 
  */
class RedisDataAnalysis {

}

object RedisDataAnalysis {
  val logger: Logger = LoggerFactory.getLogger(RedisDataAnalysis.getClass)

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    //    val filePath = "/data/logs/redis-data.csv"
    val filePath = "/data/logs/4444-10.159.39.97.csv"
    import org.apache.flink.api.scala._ // 隐式转换

    // 读取数据源文件
    val dataSource: DataSet[(String, String)] = env.readTextFile(filePath, "UTF-8").map(line => {
      try {
        val arr: Array[String] = StringUtils.split(line, ",")
        val tokenKey = arr(2)
        val token1: Array[String] = StringUtils.split(tokenKey, ":")
        (token1(0), arr(3)) // key前缀，内存大小
      } catch {
        case e: Exception => {
          logger.error("读取数据源文件异常:" + line, e)
        }
          (line, "0") // key前缀，内存大小
      }
    })

    //    各种key数量统计
    val keyWordCount: DataSet[(String, Int)] = dataSource.map(x => {
      (x._1, 1)
    }).groupBy(0).sum(1)
    //    keyWordCount.print()
    //    各种key内存大小统计
    val keyMemorySize: DataSet[(String, Long)] = dataSource.map(x => {
      if (NumberUtils.isNumber(x._2)) {
        (x._1, x._2.toLong)
      } else {
        (x._1, 0L)
      }
    }).groupBy(0).sum(1)
    //    keyMemorySize.print()

    // 合并两个流
    val keyTotal1: DataSet[(String, Int, Long)] = keyWordCount.join(keyMemorySize) //
      .where(0).equalTo(0) //关联条件
      .map(x => {
      (
        (x._1._1, x._1._2, x._2._2) // 组装最终数据
        )
    }) // 输出项配置
    //      {
    //        (args1,args2) => {
    //          (args1._1,args1._2,args2._2)
    //        }
    //      }// 输出项配置,等同于map算子
    keyTotal1.print()

    //      val keyTotal:DataSet[(String,Int,Long)] = dataSource.map(x => {
    //        if(NumberUtils.isNumber(x._2)) {
    //          (x._1,1,x._2.toLong)
    //        }else {
    //          (x._1,1,0L)
    //        }
    //      }).groupBy(0).sum(1).sum(2)
    //    keyTotal.print()

    //    val result = keyTotal1
    //    val outFilePath = "file:///data/flink/hlht/redis-key-count-" + System.currentTimeMillis()
    //    result.writeAsText(outFilePath).setParallelism(1) // 放在一个分区，避免多个文件
    //    env.execute("RedisDataAnalysis") // 执行作业
  }

  def getElementByIndex(arr: Array[String], index: Int): String = {
    try {
      if (arr.length >= index) {
        return arr(index)
      }
    } catch {
      case ex: Exception => {
        logger.error("error:{}", Throwables.getStackTraceAsString(ex))
      }
    }
    ""
  }

}


