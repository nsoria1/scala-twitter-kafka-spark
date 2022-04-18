package com.nsoria

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, date_format, sum, to_timestamp, when}
import org.apache.spark.sql.streaming.StreamingQuery

trait StreamHelper {

  def totalFavorites(df: DataFrame): Int = {
    // Get the count of tweets that had been liked in the mini batch
    df
      .agg(sum(when(col("data.favorite") > 0, 1)))
      .first()
      .toString()
      .toInt
  }

  def sumRetweets(df: DataFrame): Int = {
    // Sum of the total retweets found in the mini batch
    df
      .agg(sum(col("retweets")))
      .first()
      .toString()
      .toInt
  }

  def tweetsPerMinute(df: DataFrame): Int = {
    df
      .withColumn(
        "messageTimeFormatted", 
        to_timestamp(col("messageTime"), "EEE MMM dd HH:mm:ss zzz yyyy")
      )
      .withColumn(
        "dateToGroup",
        date_format(col("messageTimeFormatted"), "yyyyMMddhhmm")
      )
      .groupBy("dateToGroup")
      .count()
      .first()
      .toString
      .toInt
  }
}