package com.wordpress.technicado.movielens

import com.wordpress.technicado.movielens.Constants._
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import scala.collection._

class MovieRatingAnalysis(sparkContext: SparkContext) {

  def getRelevantItemData(data: RDD[String]): RDD[(String, String)] = {
    data.map(row => {
      val arr: Array[String] = row.split('|')
      (arr(0), arr(1))
    })
  }

  def getRelevantRatingData(data: RDD[String]): RDD[(String, Int)] = {
    data.map(row => {
      val arr = row.split("\t")
      (arr(1), arr(2).toInt)
    })
  }

  private def getAverageRating(inputUData: RDD[(String, Int)]) = {
    val avgRatingRDD = inputUData.mapValues(x => (x, 1))
      .reduceByKey((x, y) => ((x._1 + y._1), (x._2 + y._2)))
      .mapValues(x => (x._1/x._2))
    avgRatingRDD
  }

  def averageMovieRating: RDD[(String, Int)]  = {
    Utils.readConfig("conf/movielens.properties")
    val inputUData: RDD[(String, Int)] = getRelevantRatingData(sparkContext.textFile(Utils.getString(U_DATA)))

    val itemData: RDD[(String, String)] = getRelevantItemData(sparkContext.textFile(Utils.getString(U_ITEM)))
    val itemArr: Map[String, String] = itemData.collectAsMap()
    val itemBC: Broadcast[Map[String, String]] = sparkContext.broadcast[Map[String, String]](itemArr)
    val avgRatingRDD: RDD[(String, Int)] = getAverageRating(inputUData)

    loopUpItemData(itemBC, avgRatingRDD)
  }

  private def loopUpItemData(itemBC: Broadcast[Map[String, String]],  avgRatingRDD: RDD[(String, Int)]) = {
    avgRatingRDD.map(_.swap).sortByKey(false).map(x => {
      val item: Map[String, String] = itemBC.value
      (item.get(x._2).get, x._1)
    })
  }
}
