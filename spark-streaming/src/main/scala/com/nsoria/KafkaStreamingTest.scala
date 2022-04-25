package com.nsoria

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.nsoria.Variables._

object KafkaStreamingTest extends App {

  // Initiate Spark Session
  val spark = SparkSession
    .builder()
    .master(SPARK_MASTER)
    .appName(APP_NAME)
    .getOrCreate()

  spark.sparkContext.setLogLevel(ERROR_LOG_LEVEL)

  // Read from Kafka cluster
  val initDf = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BROKER)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load()

  val resultDf = initDf
    .select(
      col("topic"),
      col("offset"),
      col("key").cast("string"),
      col("value").cast("string")
    )

   // Write output to console
    resultDf
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

}
