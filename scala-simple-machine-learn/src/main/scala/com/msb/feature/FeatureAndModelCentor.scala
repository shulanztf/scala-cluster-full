package com.msb.feature

import com.msb.util.{HBaseUtil, PropertiesUtils}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.DataFrame
import redis.clients.jedis.JedisPool




/**
  *
  */
object FeatureAndModelCentor {

  /**
    * 将训练集的数据存储到HBase中  ctr_feature表
    * @param features
    */
  def updateFeatureCentor(features:DataFrame):Unit ={
    features.show(10)
    features.printSchema()
    /**
      * root
      * |-- userID: string (nullable = true)
      * |-- itemID: integer (nullable = false)
      * |-- duration: long (nullable = false)
      * |-- program_features: vector (nullable = true)
      * |-- province_Vector: vector (nullable = true)
      * |-- city_Vector: vector (nullable = true)
      * |-- userLabel_Vector: vector (nullable = true)
      * |-- label: integer (nullable = false)
      * |-- features: vector (nullable = true)
      */
    val tableName = PropertiesUtils.getProp("user.item.feature.centor")
    features.rdd.foreachPartition(partition => {
      val conf = HBaseUtil.getHBaseConfiguration()
      val htable = HBaseUtil.getTable(conf, tableName)
      partition.foreach(row => {
        val userID = row.getAs[String]("userID")
        val itemID = row.getAs[Int]("itemID")
        val features = row.getAs[SparseVector]("features")
        val put = new Put(Bytes.toBytes(userID+":"+itemID))
        put.addColumn(Bytes.toBytes("feature"),Bytes.toBytes("feature"), Bytes.add(Bytes.toByteArrays(features.toDense.toArray.map(x => x + "\t"))))
        htable.put(put)
      })
      htable.close()
    })
  }

  /**
    * redis缓存处理
    * @param online_model
    */
  def saveToRedis(online_model: LogisticRegressionModel): Unit = {
    val coefficients = online_model.coefficients//w1  wn
    val intercept = online_model.intercept //w0
    val argsArray = coefficients.toArray

    val jedisPool = new JedisPool("node01", 6379)
    val jedis = jedisPool.getResource

    jedis.select(3)
    //w1 .... wn
    for (index <- 0 until (argsArray.length)) {
      jedis.hset("model", (index+1).toString, argsArray(index).toString)
    }
    jedis.hset("model","0",intercept.toString)
    jedis.close()
  }

  /**
    * 训练模型处理
    * @param trainDF
    */
  def trainModel(trainDF: DataFrame): Unit = {
    val lr = new LogisticRegression()
    val model = lr.setFeaturesCol("features").setLabelCol("label").fit(trainDF)
    model.save("hdfs://node01:9000/recommond_program/models/lrModel.model")
    saveToRedis(model)
  }

  def main(args: Array[String]): Unit = {
    val features = FeaturesFactory.getLRFeatures
    features.show(10)// 数据显示
    //更新到Hbase的特征中心中
    FeatureAndModelCentor.updateFeatureCentor(features)
    //训练一个排序模型   LR
    FeatureAndModelCentor.trainModel(features)
  }
}
