package com.msb.program

import java.lang

import com.msb.algorithm.TextRank
import com.msb.util.{SegmentWordUtil, SparkSessionBase}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.{immutable, mutable}
import scala.collection.mutable.ListBuffer

/**
  * TextRank本地测试版
  */
object ComputeTextRankLocal {


  def main(args: Array[String]): Unit = {
    //通过SparkSessionBase创建Spark会话
//    val session = SparkSessionBase.createSparkSession()
//    session.sql("use program")//切换数据库
    val conf:SparkConf = new SparkConf()
    conf.setMaster("local")
    val session:SparkSession = SparkSession.builder().config(conf).appName("ComputeTextRankLocal").getOrCreate()
    import session.implicits._

    val dataRDD:RDD[String] = session.sparkContext.textFile("D:\\data\\msb\\RecommenderProgram\\item_info-10.txt")

    val data:DataFrame = dataRDD.map(x=>{
      val arr = x.split("\t")
      (new java.lang.Long(arr(0)),arr(1),arr(2),arr(3),arr(4),arr(5),arr(6),arr(7),new lang.Long(arr(8)),arr(9),arr(10),arr(11),arr(12),arr(13))
    }).toDF("id","create_date","air_date","title","name","desc","keywords","focus","length","content_model","area","language","quality","is_3d")
//    data.show() //数据查看


    //获取节目信息，然后对其进行分词
    //    val articleDF = session.sql("select * from item_info limit 20")
//    val articleDF:DataFrame = session.table("item_info")

    val seg = new SegmentWordUtil()
//    val wordsRDD = articleDF.rdd.mapPartitions(seg.segeFun)//segeFun不能写括号，否则相当于传入segeFun的出参
    val wordsRDD = data.rdd.mapPartitions(seg.segeFun)//segeFun不能写括号，否则相当于传入segeFun的出参
//    wordsRDD.toDF().show() //数据查看


    //计算每个节目 每个单词的TR值
    val tralgm = new TextRank()
    val transformGraphRDD:RDD[(Long,mutable.HashMap[String,mutable.HashSet[String]])] = wordsRDD.map(row => {
      (row._1,tralgm.transform(row._2))
    })
    val rankRDD = transformGraphRDD.map(row => {
      (row._1,tralgm.rank(row._2))
    })
//    rankRDD.toDF().show(10)//数据查看

    /**
      * 将每个节目 每个单词的TR值与对应的单词的IDF相乘
      * （1）创建广播变量  将idf_keywords_values（word idf）表数据  作为广播变量
      * （2）遍历sortByRankRDD 匹配单词，TR*IDF
      */
    val word2IDFMap = mutable.Map[String,Double]()
//    session.table("tmp_program.keyword_idf").rdd.collect().foreach(row => {
//      word2IDFMap += ((row.getAs[String]("word"), row.getAs[Double]("idf")))
//    })  // tmp_program.keyword_idf 来源于ComputeTFIDF
    val keywordTfidfRDD: DataFrame = session.read.parquet("D:\\data\\msb\\RecommenderProgram\\models\\keyword_tfidf.parquet")
//    keywordTfidfRDD.show(10) //数据查看
    keywordTfidfRDD.rdd.foreach(row => {
      word2IDFMap += ((row.getAs[String]("keywords"), row.getAs[Double]("tfidf")))
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
    val sortByWeightRDD:RDD[(Long,String,Double)] = keyWordsWithWeightsRDD.filter(_._2.size > 10) // 只取前10个
      .map(row => {
        (row._1,sortByWeights(row._2))
      }).flatMap(explode)
//    sortByWeightRDD.toDF().show(10) //数据查看

    //keyWordsWithWeightsRDD转成DF
    val word2WeightsDF:DataFrame = sortByWeightRDD.toDF("item_id", "word", "weight")
    word2WeightsDF.show(10) //数据查看
//    session.sql("use tmp_program")
//    word2WeightsDF.write.mode(SaveMode.Overwrite).insertInto("keyword_tr") //数据存储

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
