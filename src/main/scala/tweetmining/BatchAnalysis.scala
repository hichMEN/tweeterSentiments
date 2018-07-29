package tweetmining

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object BatchAnalysis {
  val conf = new SparkConf().setMaster("local[6]").setAppName("Spark batch processing")
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")
  def main(args: Array[String]): Unit = {
    println("Twitter data batch processing")
    val tweets: RDD[String] = sc.textFile("1Mtweets_en.txt")

    val tweetSentiment: RDD[(String,Double)] = tweets.map(tweet => (tweet,TweetUtilities.getSentiment(tweet)))
//    tweetSentiment.foreach(println)
   /* val  tweetSentimentSens : RDD[String] = tweetSentiment.map(sentiment => if(sentiment>0) "Happy" else if(sentiment==0) "neutral" else "Sad")
    tweetSentimentSens.foreach(println)*/
    val tweetTag: RDD[(String, Set[String])] = tweets.map(tweet => (tweet,TweetUtilities.getHashTags(tweet)))
//      tweetTag.foreach(println)

    val tweetHashTag: RDD[String] = tweets.flatMap(tweet => TweetUtilities.getHashTags(tweet))
//    tweetHashTag.foreach(println)

    val hashTagSentiment: RDD[(String,Double)] = tweetSentiment.flatMap(pair => TweetUtilities.getHashTags(pair._1).map(t => (t,pair._2)))
      .reduceByKey(_+_)
      .sortBy(_._2)
      hashTagSentiment.take(10).foreach(println)
  }
}