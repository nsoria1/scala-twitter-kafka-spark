package com.nsoria

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.from_json

object KafkaStreaming extends App with StreamHelper {

  // Initiate Spark Session
  val spark = SparkSession
    .builder()
    .master("local")
    .appName("KafkaReader")
    .getOrCreate()

  // Set error log to avoid too verbose console log
  spark.sparkContext.setLogLevel("ERROR")
  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

  val schema = new StructType()
    .add("username", StringType)
    .add("message", StringType)
    .add("favorite", IntegerType)
    .add("retweets", IntegerType)
    .add("messageTime", StringType)

  // Read from Kafka cluster
  val initDf = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("subscribe", "twitter")
    .option("startingOffsets", "earliest")
    .load()
    .select(
      col("topic"),
      col("offset"),
      from_json(col("value").cast("string"), schema).alias("data")
    )

  // Printing Dataframe schema from Kafka
  initDf.printSchema()

  // Write output to console
  val rawData = initDf.
    select(
      col("topic"),
      col("offset"),
      col("data.*"))
    .writeStream
    .format("console")
    .start()

  // Write aggregation outputs
  val result = totalFavorites(initDf)
  println(result)

  // Await manual stream stop
  rawData.awaitTermination()
}
