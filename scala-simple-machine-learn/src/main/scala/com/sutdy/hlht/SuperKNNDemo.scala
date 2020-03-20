package com.sutdy.hlht

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.immutable.TreeSet
import scala.collection.mutable.ListBuffer

/**
  *
  */
object SuperKNNDemo {

  def main(args: Array[String]): Unit = {

    //1.初始化
    val session = SparkSession.builder.appName("SuperKNNDemo").master("local").getOrCreate()
    import session.implicits._ // 隐式转换，spark-sql用
    val K = 3

    //2.读取数据,封装数据
    val dataSource = session.sparkContext.textFile("file:///D:\\data\\hlht\\point\\points-data-label\\points-label-train-20200319.txt")
    val data = dataSource.map(line => {
      val array = line.split("\t")
//      LabelPoint(parts(0), [(parts(2))])
    val vector: Vector = Vectors.dense(array(2).toDouble, array(3).toDouble) //特征
      LabeledPoint(array(1).toDouble, vector)
    })
    data.toDF().show(10) //数据显示

    //3.过滤出样本数据和测试数据  //数据分为训练数据(60%)，测试数据(40%)
    val splits = data.randomSplit(Array(0.8, 0.2), seed = 11L)
    val testData = splits(1) //测试数据
    val sampleData = splits(0) //训练数据

    //4.将testData封装到广播变量做一个优化
    val bc_testData = session.sparkContext.broadcast(testData.collect())

    //5.求每一条测试数据与样本数据的距离----使用mapPartitions应对大量数据集进行优化
    //    val distance: RDD[(String,(Double,String))] = sampleData.mapPartitions(iter => {
    //      val bc_points = bc_testData.value
    //      iter.flatMap(x => bc_points.map(point2 => (point2.mkString(","), (getDistance(point2, x.point),x.label))))
    //    })
    val distance: RDD[(String, (Double, String))] = sampleData.mapPartitions(iter => {
      val bc_points = bc_testData.value // 测试数据
      val rest = new ListBuffer[(String, (Double, String))]
      for (point <- iter) {
        for (point2 <- bc_points) {
          rest += ((point2.label.toString, (getDistance(point2.features.toArray, point.features.toArray), point.label.toString)))
        }
      }
      rest.iterator
    })
    //    distance.toDF().show(10) //数据显示

    //6.求距离最小的k个点,使用aggregateByKey---先分局内聚合,再全局聚合
    val rdd1: RDD[(String, TreeSet[(Double, String)])] = distance.aggregateByKey(TreeSet[(Double, String)]())(
      (splitSet: TreeSet[(Double, String)], elem: (Double, String)) => {
        val newSet = splitSet + elem //TreeSet默认是有序的(升序)
        newSet.take(K)
      },
      (splitSet1: TreeSet[(Double, String)], splitSet2: TreeSet[(Double, String)]) => {
        (splitSet1 ++ splitSet2).take(K)
      }
    )
    //    rdd1.toDF().show(10) //数据显示
    //7.取出距离最小的k个点中出现次数最多的label---即为样本数据的label
    val rdd2 = rdd1.map(x => {
      (
        x._1,
        x._2.toArray.map(_._2).groupBy(y => y).map(z => (z._1, z._2.length)).toList.sortBy(_._2).map(_._1).take(1).mkString(",")
      )
    })
    rdd2.toDF().show(10) //数据显示

    session.stop()
  }


//  case class LabelPoint(label: String, point: Array[Double])

  import scala.math._

  def getDistance(x: Array[Double], y: Array[Double]): Double = {
    sqrt(x.zip(y).map(z => pow(z._1 - z._2, 2)).sum)
  }

}
