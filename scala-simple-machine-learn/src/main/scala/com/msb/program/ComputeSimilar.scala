package com.msb.program

import org.apache.hadoop.hbase.util.Bytes
import com.msb.util.{HBaseUtil, PropertiesUtils, SparkSessionBase}
import org.apache.hadoop.hbase.client.Put
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.feature.{BucketedRandomProjectionLSH, Word2VecModel}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection._
import scala.collection.mutable.ArrayBuffer

/**
  *
  */
object ComputeSimilar {

  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSessionBase.createSparkSession()
    session.sql("use tmp_program")
    import session.implicits._
    val keyWord2WeightDF = session.table("keyWord2WeightDF")
    val word2Weight: Map[String, Double] = keyWord2WeightDF.rdd.map(row => {
      val itemID: Int = row.getAs[Int]("item_id")
      val word: String = row.getAs[String]("word")
      val tr: Double = row.getAs[Double]("tfidf")
      (itemID + "_" + word, tr) //格式：(节目id_关键鉰，tfidf值)
    }).collect().toMap
    val word2WeightBroad: Broadcast[Map[String, Double]] = session.sparkContext.broadcast(word2Weight)

    val word2VecModel: Word2VecModel = Word2VecModel.load("hdfs://node01:9000/recommond_program/models/w2v.model") // 加载模型
    val word2VecMap: Map[String, breeze.linalg.DenseVector[Double]] = word2VecModel.getVectors.collect().map(row => {
      val vector: breeze.linalg.DenseVector[Double] = breeze.linalg.DenseVector(row.getAs[DenseVector]("vector").toArray)
      val word = row.getAs[String]("word")
      (word, vector)
    }).toMap
    val word2VecMapBroad = session.sparkContext.broadcast(word2VecMap)

    val word2Index: Map[String, Int] = session.table("keyword_idf").rdd.map(row => {
      val index = row.getAs[Int]("index")
      val word = row.getAs[String]("word")
      (word, index) // (关键词，词袋位置)
    }).collectAsMap()
    val word2IndexBroad = session.sparkContext.broadcast(word2Index)

    val keyWordDF: DataFrame = session.sql("select * from item_keyword limit 1000")
    val featuresDF: DataFrame = keyWordDF.map(row => {
//      val map = mutable.HashSet[String, Double]()
      val word2VecMap = word2VecMapBroad.value
      val word2Weight = word2WeightBroad.value
      val word2Index = word2IndexBroad.value
      val itemID = row.getAs[Int]("item_id")
      val keywords = row.getAs[Seq[String]]("keyword")
//      var index = 0
      val indexs = new ArrayBuffer[Int]()
      val values = new ArrayBuffer[Double]()
      for (word <- keywords) {
        val weigth = word2Weight.getOrElse(itemID + "_" + word, 1.0) //权重
        var nWht: Double = 0d // idf逆文档词频，double值
        if (word2VecMap.contains(word)) {
          val trScore = word2VecMap(word) // 关键词对应的tfidf值
          val newVector = trScore * weigth
          nWht = newVector.toArray.sum / newVector.length
        } else {
          nWht = weigth
        }
        if (word2Index.contains(word)) {
          indexs += (word2Index.get(word).get) //词袋下标向量
          values += (nWht) //idf逆文档词频向量
        }
      }
      val vector = new SparseVector(word2Index.size, indexs.toArray.sorted, values.toArray)
      (itemID, vector.toDense)
    }).toDF("item_id", "features")

    val rddArr = featuresDF.randomSplit(Array(0.7, 0.3))
    val train = rddArr(0) // 训练集
    val test = rddArr(1) // 测试集

    val brpls = new BucketedRandomProjectionLSH()
    brpls.setInputCol("features") //读取字段
    brpls.setOutputCol("hashes") //输出字段
    //桶个数
    brpls.setBucketLength(10.0)
    val model = brpls.fit(train)
    val similar = model.approxSimilarityJoin(featuresDF, featuresDF, 2.0, "EuclideanDistance")
    /**
      * 分到同一个桶中
      * +--------------------+--------------------+-----------------+
      * |            datasetA|            datasetB|EuclideanDistance|
      * +--------------------+--------------------+-----------------+
      * |[337747, [0.0,8.2...|[433272, [0.0,8.2...|              0.0|
      * |[400803, [0.0,8.2...|[364358, [0.0,8.2...|              0.0|
      * |[407580, [0.0,8.2...|[256381, [0.0,8.2...|              0.0|
      * |[43163, [0.0,8.24...|[538311, [0.0,8.2...|              0.0|
      * |[43163, [0.0,8.24...|[201201, [0.0,8.2...|              0.0|
      * |[563779, [0.0,8.2...|[114265, [0.0,8.2...|              0.0|
      * |[524706, [0.0,8.2...|[206419, [0.0,8.2...|              0.0|
      * |[330830, [0.0,8.2...|[520159, [0.0,8.2...|              0.0|
      * |[418635, [0.0,8.2...|[508540, [0.0,8.2...|              0.0|
      * |[368514, [0.0,8.2...|[393038, [0.0,8.2...|              0.0|
      * +--------------------+--------------------+-----------------+
      */
    val tableName = PropertiesUtils.getProp("similar.hbase.table")
    similar.toDF().rdd.foreachPartition(partition => {
      val conf = HBaseUtil.getHBaseConfiguration()
      val htable = HBaseUtil.getTable(conf, tableName)
      for (row <- partition) {
        if (row.getAs[Double]("EuclideanDistance") < 1) {
          val aItemID = row.getAs[Row]("datasetA").getAs[Int](0)
          val bItemID = row.getAs[Row]("datasetB").getAs[Int](0)
          val dist = row.getAs[Double]("EuclideanDistance")
          if (aItemID != bItemID) {
            val put = new Put(Bytes.toBytes(aItemID + ""))
            put.addColumn(Bytes.toBytes("similar"), Bytes.toBytes(bItemID + ""), Bytes.toBytes(dist + ""))
            htable.put(put)
          }
        }
      }
    })
    session.close()
  }

}
