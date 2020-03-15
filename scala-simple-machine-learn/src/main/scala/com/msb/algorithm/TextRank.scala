package com.msb.algorithm

import scala.collection.mutable
import scala.util.control.Breaks._

/**
  * TextRank类，处理关键词的上下文语义，可参照PageRank
  */
class TextRank extends Serializable {
  var numKeyword: Int = 10 //窗口大小
  /* 关键词个数 */
  var d: Double = 0.85f
  /* 阻尼系数 */
  var max_iter: Int = 200
  /* 最大迭代次数 */
  var min_diff: Double = 0.001f
  /* 最小变化区间 */
  private final var index: Int = 0

  /**
    * 获取文本对应的单词关系向量
    *
    * @param document
    * @return
    */
  def transform(document: Iterable[_]): mutable.HashMap[String, mutable.HashSet[String]] = {
    val keyword: mutable.HashMap[String, mutable.HashSet[String]] = mutable.HashMap.empty[String, mutable.HashSet[String]]
    val que = mutable.Queue.empty[String]
    document.foreach(term => {
      val word = term.toString
      //
      if (!keyword.contains(word)) {
        /* 初始化，对每个分词分配一个 HashSet 空间*/
        keyword.put(word, mutable.HashSet.empty[String])
      }
      que.enqueue(word) //加入队列
      if (que.size > 5) {
        que.dequeue() // 队列大小为5，超出时，删除最旧的
      }
      for (w1 <- que) {
        for (w2 <- que) {
          if (!w1.equals(w2)) {
            keyword.apply(w1).add(w2) // 两单词不一样时，互相放入对方的向量中
            keyword.apply(w2).add(w1)
          }
        }
      }
    })
    keyword
  }

  /**
    * TextRank值计算
    * @param document
    * @return
    */
  def rank(document: mutable.HashMap[String, mutable.HashSet[String]]): mutable.HashMap[String, Double] = {
    var score = mutable.HashMap.empty[String, Double]
    breakable({
      for (iter <- 1 to max_iter) {
        val tmpScore = mutable.HashMap.empty[String, Double]
        var max_diff: Double = 0F
        for (word <- document) {
          tmpScore.put(word._1, 1 - d)

          for (element <- word._2) {
            val size = document.apply(element).size//获取对应的关键词，在词袋中的数量
            if (0 == size) {
              println("document.apply(element).size == 0 :element: " + element + "keyword: " + word._1)
            }
            if (word._1.equals(element)) {
              println("word._1.equals(element): " + element + "keyword: " + word._1)
            }
            if (!word._1.equals(element) && 0 != size) {
              /* 计算，这里计算方式可以和TextRank的公式对应起来 */
              tmpScore.put(word._1, tmpScore.apply(word._1) + ((d / size) * score.getOrElse(word._1, 0.0d)))
            }
          }
          /* 取出每次计算中变化最大的值，用于下面得比较，如果max_diff的变化低于min_diff，则停止迭代 */
          max_diff = Math.max(max_diff, Math.abs(tmpScore.apply(word._1) - score.getOrElse(word._1, 0.0d)))
        }
        score = tmpScore
        if (max_diff <= min_diff) {
          break()
        }
      }

    })
    score
  }

  def sortByRank(doc: mutable.HashMap[String, Double]): Map[String, Double] = {
    val mapDoc = doc.toSeq
    val reverse = mapDoc.sortBy(-_._2).take(10).toMap
    reverse
  }
}
