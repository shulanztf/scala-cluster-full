package com.msb.program

import com.msb.algorithm.TextRank
import com.msb.util.{SegmentWordUtil, SparkSessionBase}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SaveMode

import scala.collection.{immutable, mutable}
import scala.collection.mutable.ListBuffer

/**
  *
  */
object ComputeTextRank {

  def main(args: Array[String]): Unit = {
    //通过SparkSessionBase创建Spark会话
    val session = SparkSessionBase.createSparkSession()
    session.sql("use program")//切换数据库
    //获取节目信息，然后对其进行分词
    //    val articleDF = session.sql("select * from item_info limit 20")
    val articleDF = session.table("item_info")
    val seg = new SegmentWordUtil()
    val wordsRDD = articleDF.rdd.mapPartitions(seg.segeFun)//segeFun不能写括号，否则相当于传入segeFun的出参

    //计算每个节目 每个单词的TR值
    val tralgm = new TextRank()
    val transformGraphRDD = wordsRDD.map(row => {
      (row._1,tralgm.transform(row._2))
    })
    val rankRDD = transformGraphRDD.map(row => {
      (row._1,tralgm.rank(row._2))
    })
    //    rankRDD.foreach(println)

    /**
      * 将每个节目 每个单词的TR值与对应的单词的IDF相乘
      * （1）创建广播变量  将idf_keywords_values（word idf）表数据  作为广播变量
      * （2）遍历sortByRankRDD 匹配单词，TR*IDF
      */
    val word2IDFMap = mutable.Map[String,Double]()
    session.table("tmp_program.keyword_idf").rdd.collect().foreach(row => {
      word2IDFMap += ((row.getAs[String]("word"), row.getAs[Double]("idf")))
    })
    val word2IDFBroad:Broadcast[mutable.Map[String,Double]] = session.sparkContext.broadcast(word2IDFMap)//配置广播变量

    //将每篇文章中每个单词的tr*对用的IDF值  作为筛选关键词的依据
    val keyWordsWithWeightsRDD = rankRDD.map(data => {
      val itemID = data._1//单词词袋下标
      val word2TR = data._2
      val word2IDFMap = word2IDFBroad.value//广播变量返回值
      val list = new ListBuffer[(Long,String,Double)]
      val word2Weights = word2TR.map(row => {
        val word = row._1
        val tr = row._2//tr词频值
        var weights = 0d
        if(word2IDFMap.contains(word)) {
          weights = word2IDFMap(word) * tr
        }else {
          weights = tr
        }
        (word,weights)
      })
      (itemID,word2Weights)
    })

    //根据混合的weight值排序，选择topK个单词
    val sortByWeightRDD = keyWordsWithWeightsRDD.filter(_._2.size > 10) // 只取前10个
      .map(row => {
        (row._1,sortByWeights(row._2))
      }).flatMap(explode)

    //keyWordsWithWeightsRDD转成DF
    import session.implicits._
    val word2WeightsDF = sortByWeightRDD.toDF("item_id", "word", "weight")
    session.sql("use tmp_program")
    word2WeightsDF.write.mode(SaveMode.Overwrite).insertInto("keyword_tr")

    /**
      * create table keyword_tr(
      * item_id Int comment "index",
      * word STRING comment "word",
      * tr Double comment "idf"
      * )
      * COMMENT "keyword_tr"
      * row format delimited fields terminated by ','
      * LOCATION '/user/hive/warehouse/tmp_program.db/keyword_tr';
      */
    session.close()
  }

  /**
    * 关键词数据组装，格式：(单词词袋id，单词，idf*tr值)
    * @param data
    * @return
    */
  def explode(data: (Long, Map[String, Double])): Iterator[(Long,String,Double)] = {
    val itemID = data._1
    val ds = data._2
    val list = new ListBuffer[(Long,String,Double)]
    for(elem <- ds) {
      list += ((itemID,elem._1,elem._2))
    }
    list.iterator
  }

  /**
    * 按权重排序
    * @param doc
    * @return
    */
  def sortByWeights(doc: mutable.HashMap[String, Double]): immutable.Map[String, Double] = {
    val mapDoc = doc.toSeq
    val reverse:immutable.Map[String, Double] = mapDoc.sortBy(-_._2)// 倒序排列
      .take(10).toMap
    reverse
  }
}
