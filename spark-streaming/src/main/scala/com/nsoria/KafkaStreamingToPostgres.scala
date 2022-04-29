package com.nsoria

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import com.nsoria.Variables._

object KafkaStreamingToPostgres extends App {

  // Initiate Spark Session
  val spark = SparkSession
    .builder()
    .master(SPARK_MASTER)
    .appName(APP_NAME)
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  def readStreamData(spark: SparkSession): DataFrame = {

//    val schema = new StructType()
//      .add("username", StringType)
//      .add("message", StringType)
//      .add("favorite", IntegerType)
//      .add("retweets", IntegerType)
//      .add("messageTime", StringType)

    val raw = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", BROKER)
      .option("subscribe", TOPIC)
      .option("startingOffsets", "earliest")
      .load()
      .select(
        col("topic"),
        col("offset"),
        col("key").cast("string"),
        col("value").cast("string"),
      )
    raw
  }

  def writeStreamData(df: DataFrame): StreamingQuery = {
    // Printing Dataframe schema from Kafka
    df.printSchema()

    println(s"Is the current dataframe a streaming one? ${df.isStreaming.toString}")

    df
      .writeStream
      .foreachBatch { (batch: DataFrame, batchId: Long) =>
        batch.write
          .format("jdbc")
          .option("driver", "org.postgresql.Driver")
          .option("checkpointLocation", CHECKPOINT_PATH)
          .option("url", POSTGRES_URL)
          .option("dbtable", "kafkadata")
          .option("user", POSTGRES_USER)
          .option("password", POSTGRES_PW)
          .mode("append")
          .save()
      }
      .start()
  }

  val raw: DataFrame = readStreamData(spark = spark)
  writeStreamData(raw).awaitTermination()
}
