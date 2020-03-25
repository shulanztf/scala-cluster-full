package com.msb.program

import java.lang

import com.msb.util.{SegmentWordUtil, SparkSessionBase}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  */
object ComputeWTVLocal {

  def main(args: Array[String]): Unit = {
    //通过SparkSessionBase创建Spark会话
//    val session = SparkSessionBase.createSparkSession()
    val conf:SparkConf = new SparkConf()
    conf.setMaster("local")
    val session:SparkSession = SparkSession.builder().config(conf).appName(this.getClass.getSimpleName).getOrCreate()
    import session.implicits._
    import session.implicits._
//    session.sql("use program")
    //获取节目信息，然后对其进行分词
    //    val articleDF = session.sql("select * from item_info limit 20")
//    val articleDF = session.table("item_info")
    val dataRDD:RDD[String] = session.sparkContext.textFile("/data/msb/RecommenderProgram/item_info-10.txt")
    val articleDF:DataFrame = dataRDD.map(x=>{
      val arr = x.split("\t")
      (new java.lang.Long(arr(0)),arr(1),arr(2),arr(3),arr(4),arr(5),arr(6),arr(7),new lang.Long(arr(8)),arr(9),arr(10),arr(11),arr(12),arr(13))
    }).toDF("id","create_date","air_date","title","name","desc","keywords","focus","length","content_model","area","language","quality","is_3d")
    //    data.show(10) //数据查看

    val seg = new SegmentWordUtil()
    val words_df = articleDF.rdd.mapPartitions(seg.segeFun).toDF("item_id", "words")

    /**
      * vectorSize: 词向量长度
      * minCount：过滤词频小于5的词
      * windowSize：window窗口大小
      */
    val w2v = new Word2Vec()
    w2v.setInputCol("words")
    w2v.setOutputCol("model")
    w2v.setVectorSize(128)
    w2v.setMinCount(3)
    w2v.setWindowSize(5)

    val w2vModel: Word2VecModel = w2v.fit(words_df)
    val df1:DataFrame = w2vModel.transform(words_df)
    df1.show(10,false) // 数据显示

//    w2vModel.write.overwrite().save("hdfs://node01:9000/recommond_program/models/w2v.model")
//    session.read.parquet("hdfs://node01:9000/recommond_program/models/w2v.model/data/*").show(false)
    session.close()
  }

}
