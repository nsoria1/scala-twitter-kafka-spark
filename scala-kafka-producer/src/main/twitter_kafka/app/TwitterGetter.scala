package main.twitter_kafka.app

import twitter4j._
import twitter4j.auth.AccessToken
import org.json4s.native.Serialization.write
import org.json4s.{DefaultFormats, Formats}
import scala.collection.JavaConverters._

trait TwitterInstance {
  // Authorising with your Twitter Application credentials
  val twitter: Twitter = new TwitterFactory().getInstance()
  twitter.setOAuthConsumer("IY52OjqYfhaS0qlTowyJDtMR3","GutTXJ9gLGft5BttdPREcy1W1wkuZ67uZFT5S7bym4CvXoTssi")
  twitter.setOAuthAccessToken(new AccessToken("577426475-VvMf4L2YigOxNevWkQDJrSjTIIhJo2lPIEnttzi5", "VPXZJ42I8IKOaSqf3i1hdeRL5rajw78VmHnESGmKMMLm2"))
}

trait GetTweets {
  def getTweets(twitter: Twitter, q: String): List[Map[String, Any]] = {
    // Return list of tweets based on filter
    val statuses = twitter.search(new Query(q)).getTweets
    // Create a Set collection with the data required
    statuses.asScala.map(status =>
      Map("username" -> status.getUser.getScreenName,
        "message" -> status.getText,
        "favorite" -> status.getFavoriteCount,
        "retweets" -> status.getRetweetCount,
        "messageTime" -> status.getCreatedAt.toString)
    ).toList
  }
}

trait MsgToJson {
  case class MessageStruct(username: String, message: String, favorite: Int, retweets: Int, messageTime: String)

  def stringToJson (msg: Map[String, Any]): String = {
    // Making Twitter message response as Json like
    val data = new MessageStruct(
      username = msg("username").toString,
      message = msg("message").toString,
      favorite = msg("favorite").toString.toInt,
      retweets = msg("retweets").toString.toInt,
      messageTime = msg("messageTime").toString
    )
    implicit val formats: AnyRef with Formats = DefaultFormats
    write(data)
  }
}

object TwitterGetter extends App with TwitterInstance with GetTweets with MsgToJson {
  def main(q: String): List[String] = {
    val tweetResults = getTweets(twitter, q)
    val formattedTws = tweetResults.map(x => stringToJson(x))
    formattedTws
  }
  val res = main("bad bunny")
  println(res)
  res.foreach(x => println(x))
}