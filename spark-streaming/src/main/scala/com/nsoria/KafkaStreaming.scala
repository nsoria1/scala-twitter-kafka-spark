package com.nsoria

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object KafkaStreaming extends App {

  // Initiate Spark Session
  val spark = SparkSession
    .builder()
    .master("local")
    .appName("KafkaReader")
    .getOrCreate()

  // Set error log to avoid too verbose console log
  spark.sparkContext.setLogLevel("ERROR")

  // Read from Kafka cluster
  val initDf = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("subscribe", "twitter")
    .option("startingOffsets", "earliest")
    .load()

  // Printing Dataframe schema from Kafka
  initDf.printSchema()

  // Parse data from Kafka
  val inputDf = initDf.selectExpr("CAST(value as STRING)")

  val schema = new StructType()
    .add("username", StringType)
    .add("message", StringType)
    .add("favorite", IntegerType)
    .add("retweets", IntegerType)
    .add("messageTime", IntegerType)

  val parsedDf = inputDf.select(from_json(col("value"), schema).as("data"))
    .select("data.*")


  parsedDf.writeStream
    .format("console")
    .outputMode("complete")
    .trigger(ProcessingTime("10 seconds"))
    .start()
}
