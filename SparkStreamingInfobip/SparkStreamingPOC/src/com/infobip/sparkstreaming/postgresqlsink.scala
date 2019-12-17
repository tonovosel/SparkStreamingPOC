package com.infobip.sparkstreaming

import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.io.IOException


class JDBCSink(url: String, user:String, pwd:String) extends org.apache.spark.sql.ForeachWriter[org.apache.spark.sql.Row]{
  val driver = "org.postgresql.Driver"
  var connection:java.sql.Connection = _
  var statement:java.sql.Statement = _
  
  def myLogger(message: String){
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
    
    logger.error(message)
  }
 

  def open(partitionId: Long, version: Long):Boolean = {
      Class.forName(driver)
      connection = java.sql.DriverManager.getConnection(url, user, pwd)
      statement = connection.createStatement
      true
  }

  def process(value: org.apache.spark.sql.Row): Unit = {
    
  try{
  statement.executeUpdate("INSERT INTO public.smstraffichighfreq (windowstarttime, windowendtime, smschannelid, gatewayid, networkid, smscount)" + 
                           " VALUES ('" + value(0) + "','" + value(1) + "'," + value(2) + "," + value(3) + "," + value(4) + "," + value(5) + ")" + 
                           " ON CONFLICT on constraint smstraffichighfreq_pkey" + 
                           " DO UPDATE SET smscount = EXCLUDED.smscount;")
  }
  catch{
    case ex: IOException => myLogger("Writing to database failed.")
  }
  }

  def close(errorOrNull:Throwable):Unit = {
      connection.close
  }
}