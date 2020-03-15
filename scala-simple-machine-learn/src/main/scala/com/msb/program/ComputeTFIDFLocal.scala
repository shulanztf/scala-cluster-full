package com.msb.program

import java.lang

import com.msb.util.{SegmentWordUtil, SparkSessionBase}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, IDF, IDFModel}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * it-idf数据处理，本地模式
  */
object ComputeTFIDFLocal {

  def main(args: Array[String]): Unit = {
    //通过SparkSessionBase创建Spark会话
//    val session:SparkSession = SparkSessionBase.createSparkSession()
    val conf:SparkConf = new SparkConf()
    conf.setMaster("local")
    val session:SparkSession = SparkSession.builder().config(conf).appName("ComputeTFIDFLocal").getOrCreate()
    import session.implicits._
    /**
      * 查询hive哪一个数据库有两种方式：
      *   1、sql("use database")
      *   2、sql(select * from program.item_info)
      */
//    session.sql("use program")
//    //获取节目信息，然后对其进行分词
//    val articleDF = session.sql("select * from item_info limit 20")
//        val articleDF = session.table("item_info")

    //分词
    val seg = new SegmentWordUtil()
    //    val words_df:RDD[(Long,List[String])] = articleDF.rdd.mapPartitions(seg.segeFun)
//    val words_df:DataFrame = articleDF.rdd.mapPartitions(seg.segeFun).toDF("item_id", "words")
    ////    words_df.show(false)


    val dataRDD:RDD[String] = session.sparkContext.textFile("/data/msb/RecommenderProgram/item_info-10.txt")
    val data:DataFrame = dataRDD.map(x=>{
      val arr = x.split("\t")
      (new java.lang.Long(arr(0)),arr(1),arr(2),arr(3),arr(4),arr(5),arr(6),arr(7),new lang.Long(arr(8)),arr(9),arr(10),arr(11),arr(12),arr(13))
    }).toDF("id","create_date","air_date","title","name","desc","keywords","focus","length","content_model","area","language","quality","is_3d")
    //    data.show(10) //数据查看

        val words_df:DataFrame = data.rdd.mapPartitions(seg.segeFun).toDF("item_id", "words")
//        words_df.show(10) //数据查看


    //创建CountVectorizer对象，统计所有影响的词，形成词袋
    val countVectorizer = new CountVectorizer()
    countVectorizer.setInputCol("words")//读取字段
    countVectorizer.setOutputCol("features")//输出字段
    countVectorizer.setVocabSize(10000)//词袋大小10000，统计频率最高的前10000
    //词必须出现在至少一篇文章中  如果是一个0-1的数字，则代表概率
    countVectorizer.setMinDF(1.0)// 词频提取下限，1.0至少1篇文章出现；0.3至少30%的文章中出现

    //训练词袋模型
    var cvModel:CountVectorizerModel = countVectorizer.fit(words_df)
    //    //保存词袋模型到hdfs上
    //        cvModel.write.overwrite().save("hdfs://node01:9000/recommond_program/models/CV.model")
    //
    //    //通过spark sql读取模型内容
    //    session.read.parquet("hdfs://node01:9000/recommond_program/models/CV.model/data/*").show()
    //    //这是所有的词
    //        cvModel.vocabulary.foreach(println)

    val cv_result:DataFrame = cvModel.transform(words_df)
//    cv_result.show(10,false)//显示10条

    //创建IDF对象
    val idf = new IDF()
    idf.setInputCol("features")
    idf.setOutputCol("features_tfidf")
    //计算每个词的逆文档频率
    val idfModel:IDFModel = idf.fit(cv_result)
//    idfModel.write.overwrite().save("hdfs://node01:9000/recommond_program/models/IDF.model")
    idfModel.write.overwrite().save("D:\\data\\msb\\RecommenderProgram\\models\\IDF.model")


    /**
      * tf：w1：10   w1：100
      *
      * idf基于整个语料库计算出来的
      * word ： idf值
      */
//    session.read.parquet("hdfs://node01:9000/recommond_program/models/IDF.model/data").show(10,false)
    session.read.parquet("/data/msb/RecommenderProgram/models/IDF.model/data")
//      .show(10,false)

    /**
      * 将每个单词对应的IDF（逆文档频率） 保存在Hive表中
      */
    //整理数据格式（index,word,IDF）
    val keywordsWithIDFList = new ListBuffer[(Int,String,Double)]
    val words:Array[String] = cvModel.vocabulary
    val idfs:Array[Double] = idfModel.idf.toArray
    for(index <- 0 until(words.length)) {
      keywordsWithIDFList += ((index,words(index),idfs(index)))
    }
    //保存数据
//    session.sql("use tmp_program")
//    session.sparkContext.parallelize(keywordsWithIDFList).toDF("index", "keywords", "idf")
//      .write.mode(SaveMode.Overwrite).insertInto("keyword_idf")
    val keyword_idf:DataFrame = session.sparkContext.parallelize(keywordsWithIDFList).toDF("index", "keywords", "idf")
    keyword_idf.write.mode(SaveMode.Overwrite).save("/data/msb/RecommenderProgram/models/keyword_idf")

    //CVModel->CVResult->IDFModel->CVResult->TFIDFResult

    val tfIdfResult:DataFrame = idfModel.transform(cv_result)
//    tfIdfResult.show(10)//数据查看

    //根据TFIDF来排序
    val keyword2TFIDF:DataFrame = tfIdfResult.rdd.mapPartitions(part => {
      val rest = new ListBuffer[(Long,Int,Double)]
      val topN = 20 // 取前20

      while (part.hasNext) {
        val row = part.next()
        val idfVals:List[Double] = row.getAs[SparseVector]("features_tfidf").values.toList
        val tmpList = new ListBuffer[(Int,Double)]

        for(i <- 0 until(idfVals.length)) {
          tmpList += ((i,idfVals(i)))
        }

        val buffer:ListBuffer[(Int,Double)] = tmpList.sortBy(_._2).reverse
        for(item <- buffer.take(topN)) {
          rest += ((row.getAs[Long]("item_id"),item._1,item._2))
        }
      }
      rest.iterator
    }).toDF("item_id", "index", "tfidf")
//    keyword2TFIDF.show(10) //数据查看

    keyword_idf.createTempView("keyword_idf") //临时视图
    keyword2TFIDF.createGlobalTempView("keywordsByTable") //全局视图
    //获取索引对应的单词，组织格式 保存Hive表
//    session.sql("select * from keyword_idf a join global_temp.keywordsByTable b on a.index = b.index")
//      .select("item_id", "word", "tfidf")
//      .write.mode(SaveMode.Overwrite).insertInto("keyword_tfidf")
       val result:DataFrame = session.sql("select * from keyword_idf a join global_temp.keywordsByTable b on a.index = b.index")
          .select("item_id", "keywords", "tfidf")
//        result.show(10)//数据查看
    result.coalesce(1).write.mode(SaveMode.Overwrite).parquet("/data/msb/RecommenderProgram/models/keyword_tfidf.parquet")//数据存储本地

    session.close()
  }

}
