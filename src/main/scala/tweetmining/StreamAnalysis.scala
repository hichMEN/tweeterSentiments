package tweetmining

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.dstream.DStream

object StreamAnalysis {
  val conf = new SparkConf().setMaster("local[6]").setAppName("Spark batch processing")
  val sc = new SparkContext(conf)
  val streamingContext = new StreamingContext(sc, Seconds(10))
  sc.setLogLevel("WARN")
  def main(args: Array[String]): Unit = {
    println("Twitter data stream processing")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "twitter-consumer",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))

    val topics = Array("tweets")
    val tweetStream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))

    var usable = tweetStream.map(_.value())

    usable.foreachRDD(tweets => {
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
    })


    streamingContext.start()
    streamingContext.awaitTermination()
  }
}