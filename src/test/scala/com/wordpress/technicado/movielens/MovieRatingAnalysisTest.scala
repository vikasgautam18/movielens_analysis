package com.wordpress.technicado.movielens

import java.io.File

import com.wordpress.technicado.movielens.Constants.U_ITEM
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.scalatest.{BeforeAndAfterAll, FunSpec}

class MovieRatingAnalysisTest extends FunSpec with BeforeAndAfterAll{

  var sparkConf: SparkConf = _
  var sparkContext: SparkContext = _
  var dataRDD: RDD[(String, Int)] = _
  var itemRDD: RDD[(String, String)] = _
  var movieRatingAnalysis: MovieRatingAnalysis = _
  var dataPath: String = _
  var itemPath: String = _

  override def beforeAll(): Unit = {
    sparkConf = new SparkConf
    sparkConf.setMaster("local")
    sparkConf.setAppName("WeatherDataAnalysisTest")
    sparkContext = SparkContext.getOrCreate(sparkConf)
    movieRatingAnalysis = new MovieRatingAnalysis(sparkContext)

    dataPath = "file://" + new File("src/test/resources/u.data").getAbsolutePath
    itemPath = "file://" + new File("src/test/resources/u.item").getAbsolutePath

    itemRDD = movieRatingAnalysis.getRelevantItemData(sparkContext.textFile(itemPath))
    dataRDD = movieRatingAnalysis.getRelevantRatingData(sparkContext.textFile(dataPath))
  }

  describe("MovieRatingAnalysisTest") {
    it("should pull relevant info from item dataset"){
      val data = Seq(("196", "Dead Poets Society (1989)"),
        ("186", "Blues Brothers, The (1980)"),
        ("22", "Braveheart (1995)"),
        ("244", "Smilla's Sense of Snow (1997)"),
        ("166", "Manon of the Spring (Manon des sources) (1986)"))
      val expectedRDD = sparkContext.parallelize(data)
      assertResult(expectedRDD.collect)(itemRDD.collect)
    }
    it("should pull relevant info from ratings dataset"){
      val data = Seq(("196", 3),
        ("186", 3),
        ("22", 1),
        ("244", 2),
        ("166", 1))
      val expectedRDD = sparkContext.parallelize(data)
      assertResult(expectedRDD.collect)(dataRDD.collect)
    }
  }

}
