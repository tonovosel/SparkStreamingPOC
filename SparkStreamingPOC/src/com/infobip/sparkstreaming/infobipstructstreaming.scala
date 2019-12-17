package com.infobip.sparkstreaming

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.streaming.kafka._

import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.io.IOException


object infobipstructstreaming {
  
  def main(args: Array[String]){
    
    //Avoid printing all Spark log messages to console
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    
    
    
    //Preparing logger config
    var  layout = new PatternLayout()
    var conversionPattern = "%d{yyyy/MM/dd HH:mm:ss} %p %c{1}: %m%n"
    layout.setConversionPattern(conversionPattern)
    
    var fileAppender = new FileAppender()
    fileAppender.setFile("streaming.log")
    fileAppender.setLayout(layout)
    fileAppender.activateOptions()
        
    var logger = Logger.getLogger(this.getClass())
    logger.addAppender(fileAppender)
    
    
    
    logger.info("Creating Spark session...")
    val spark = SparkSession
      .builder
      .appName("SMSTrafficStreaming")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp")
      .config("spark.sql.streaming.checkpointLocation", "file:///C:/sparkcheckpoint")
      .getOrCreate()
    
    val schema = new StructType()
      .add("SendDateTime",StringType)
      .add("SMSChannelId",IntegerType)
      .add("GatewayId",IntegerType)
      .add("NetworkId",IntegerType)
      .add("SMSCount",IntegerType)
      
    import spark.implicits._
    
    val readQuery= spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "infobip")
      .load()
      
    logger.info("Reading stream from Kafka...")
    val convertedMessagesDF = readQuery.select(from_json(col("value").cast("string"), schema).as("data"))
                                        .select("data.*")
  
    val renamedDF = convertedMessagesDF
                      .withColumn("SendDateTime",to_utc_timestamp(to_timestamp(col("SendDateTime"), "yyyy-MM-dd'T'HH:mm:ss.SSS"), "UTC"))
                      .select("SendDateTime", "SMSChannelId", "GatewayId", "NetworkId", "SMSCount")
    
    logger.info("Aggregating Dataframe under the hood...")
    val windowedDF = renamedDF.withWatermark("SendDateTime", "10 minutes") //for late data to be taken in account
                              .groupBy($"SMSChannelId", $"GatewayId", $"NetworkId", window($"SendDateTime", "5 minutes", "10 seconds"))
                              .agg(sum("SMSCount").alias("SMSCount"))
                              .withColumn("WindowStartTime", col("window").getField("start"))
                              .withColumn("WindowEndTime", col("window").getField("end"))
                              .select("WindowStartTime", "WindowEndTime", "SMSChannelId", "GatewayId", "NetworkId", "SMSCount")
                              .orderBy("WindowStartTime")
    
    
    val url="jdbc:postgresql://localhost:5432/postgres"
    val user="postgres"
    val pwd="infobip"
    val writer = new JDBCSink(url, user, pwd)
    
    val outputStreamDB=windowedDF
      .writeStream
      .foreach(writer)
      .outputMode("complete")
      .start()
    
    val outputStreamConsole = windowedDF.writeStream
      .format("console")
      .outputMode("complete")
      .option("truncate", "false")
      .option("checkpointLocation", "C:/Infobip/SparkStreamingInfobip/SparkStreamingPOC/disasterrecovery")
      .start()
     
    try{
    spark.streams.awaitAnyTermination()
    }
    catch{
      case ex: IOException => logger.error("Output streams to DB and console failed.")
    }
    spark.stop()
  }
  
}