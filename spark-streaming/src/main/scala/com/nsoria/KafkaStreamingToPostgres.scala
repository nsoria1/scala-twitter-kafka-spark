package com.nsoria

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object KafkaStreamingToPostgres extends App {

  // Initiate Spark Session
  val spark = SparkSession
    .builder()
    .master("local")
    .appName("KafkaReader")
    .getOrCreate()

//  val schema = new StructType()
//    .add("username", StringType)
//    .add("message", StringType)
//    .add("favorite", IntegerType)
//    .add("retweets", IntegerType)
//    .add("messageTime", StringType)

  // Read from Kafka cluster
  val initDf = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("subscribe", "twitter")
    //.option("startingOffsets", "earliest")
    .option("startingOffsets", "earliest")
    .load()

  // Printing Dataframe schema from Kafka
  initDf.printSchema()

  val resultDf = initDf
    .select(
      col("topic"),
      col("offset"),
      col("key").cast("string"),
      col("value").cast("string")
      //from_json(col("value").cast("string"), schema).alias("data")
  )

  // Write output to console
  //  resultDf
  //    .writeStream
  //    .format("csv")
  //    .option("format", "append")
  //    .trigger(Trigger.ProcessingTime("5 seconds"))
  //    .option("path", "data/")
  //    .option("checkpointLocation", "checkpoint/")
  //    .outputMode("append")
  //    .start()
  //    .awaitTermination()

  resultDf
    .writeStream
    .foreachBatch { (batch: DataFrame, batchId: Long) =>
      batch.write
        .format("jdbc")
        .option("driver", "org.postgresql.Driver")
        .option("url", "jdbc:postgresql://localhost:5432/postgres")
        .option("dbtable","kafkadata")
        .option("user","postgres")
        .option("password", "postgres")
        .mode("append")
        .save()
    }
    .start()
    .awaitTermination()

}
