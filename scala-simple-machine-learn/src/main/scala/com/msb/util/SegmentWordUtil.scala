package com.msb.util

import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

/**
  * id                  	bigint              	item unique key
  * create_date         	string              	create date
  * air_date            	string              	air_date
  * title               	string              	title
  * name                	string              	name
  * desc                	string              	desc
  * keywords            	string              	keywords
  * focus               	string              	focus
  * length              	bigint              	length
  * content_model       	string              	content_model
  * area                	string              	area
  * language            	string              	language
  * quality             	string              	quality
  * is_3d               	string              	is_3d
  */
class SegmentWordUtil  extends Serializable{

  def segeFun(itera:Iterator[Row]): Iterator[(Long,List[String])] = {
    val rest = new ListBuffer[(Long,List[String])]
    val analyzer = new IKAnalyzer()
    while (itera.hasNext) {
      val row = itera.next()
      val item_id = row.getAs[Long]("id")
      val desc = row.getAs[String]("desc")//描述
      val title = row.getAs[String]("title")//标题
      val name =row.getAs[String]("name")//节目名称

      //将节目的描述、标题、名字等信息合并
      val itemConten = desc + " " + title + " " + name

      //对合并的信息进行分词
      import scala.collection.JavaConversions._
      val words:List[String] = analyzer.segmentation(itemConten).toSeq.toList
      rest += ((item_id,words))
    }
    rest.iterator
  }
}

object SegmentWordUtil {

}
