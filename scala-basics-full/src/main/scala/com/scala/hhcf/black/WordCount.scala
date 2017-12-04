package com.scala.hhcf.black

import com.alibaba.fastjson.JSON
import org.codehaus.jackson.JsonParser
import net.sf.json.JSONObject
import scala.util.parsing.json.JSONObject
import net.sf.json.JSONObject
import com.alibaba.fastjson.JSONObject
import com.google.gson.JsonObject



/**
 * 单词计数器
 */
object WordCount {

  def main(args: Array[String]) {
    //    数据
    val lines = Array("aa ee dd ss ", "ee cc bb", "dd ee cc aa ", "aa cc ee cc")
 
    //    合并词条，转成数组
    val words = lines.map(f => { f.split(" ") }).filter(p => { p != "" })
    //    转成元组
    val wordT1 = words.map(x => { (x, 1) })
    //    按单词计数
    val grouped = wordT1.groupBy(f => { f._1 })
    //   转成list
    val wordList = grouped.map(f => { (f._1, f._2.length) }).toList
    //  排序
    val rslt = wordList.sortBy(f => { f._2 }).reverse
    //    println("aaaaaaaaaaaaa")
    println(rslt.toString())

  }

}