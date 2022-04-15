package com.nsoria

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, when, sum}

trait StreamHelper {

  def totalFavorites(df: DataFrame): Int = {
    // Get the count of tweets that had been liked in the mini batch
    df
      .agg(sum(when(col("favorite") > 0, true)))
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

  }

}