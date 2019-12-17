package com.infobip.sparkstreaming

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel
import java.util.Locale

import org.apache.spark.streaming.kafka._
import kafka.serializer.StringDecoder

//case class Messages(DateTimeSend: String, SMSChannelId: Int, GatewayId: Int, NetworkId: Int, SMSCount: Int)

object infobipstreaming {
  
  def main(args: Array[String]){
    
  val spark = SparkSession
    .builder
    .appName("SMSTrafficStreaming")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "file:///C:/temp")
    .config("spark.sql.streaming.checkpointLocation", "file:///C:/sparkcheckpoint")
    .getOrCreate()
    
  val sc = spark.sparkContext
  val ssc = new StreamingContext(sc, Seconds(1))
  
  val kafkaBroker = Map("metadata.broker.list" -> "localhost:9092")
  
  val kafkaTopic = List("infobip").toSet
  
  val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
  ssc, kafkaBroker, kafkaTopic).map(_._2)
  
  messages.foreachRDD(rdd =>{
    val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
    import sqlContext.implicits._
    val df = sqlContext.read.json(rdd)
  })
 
   ssc.start()
   ssc.awaitTermination()
  
  }
  
}

object SQLContextSingleton {

  @transient  private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}