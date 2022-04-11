package tests

import com.nsoria.TwitterInstance
import org.scalatest.funsuite.AnyFunSuite
import org.mockito.Mock
import org.scalatestplus.mockito.MockitoSugar

class TwitterGetterTest extends AnyFunSuite with MockitoSugar {

  test("Testing TwitterInstance Trait") {
    import com.nsoria.TwitterInstance
    val tw = new TwitterInstance {}
    println(s"Username is: ${tw.twitter.getScreenName}")
    assert(tw.twitter.verifyCredentials().getScreenName == "nariver28")
  }

  test("Handling response from Twitter") {
    import twitter4j.Twitter
    import com.nsoria.GetTweets

    // Mock Twitter connection
    val tw = mock[Twitter]

    // Create instance of GetTweets
    class GetTweetsMock extends GetTweets {
      override def getTweets(twitter: Twitter, q: String): List[Map[String, Any]] = {
        val response = List(Map("username" -> "Quetzadille",
          "message" -> "RT @karolkofler: yo en el amor soy bad bunny https://t.co/BTlq7hF5Xt",
          "favorite" -> "2",
          "retweets" -> 2,
          "messageTime" -> "Thu Apr 07 00:28:10 ART 2022"))
        response
      }
    }

    // Create new instance of mocked class
    val mockGetTweet = new GetTweetsMock()

    // Test Mock
    assert(
      mockGetTweet.getTweets(tw, "bad bunny") ==
      List(Map(
        "username" -> "Quetzadille",
        "retweets" -> 2,
        "message" -> "RT @karolkofler: yo en el amor soy bad bunny https://t.co/BTlq7hF5Xt",
        "messageTime" -> "Thu Apr 07 00:28:10 ART 2022",
        "favorite" -> "2")
      )
    )
  }

  test("Format Twitter response test") {
    import com.nsoria.MsgToJson

    // Mock trait
    class MsgToJsonMock extends MsgToJson
    val mock = new MsgToJsonMock

    // Mock data
    val mockData = Map(
      "username" -> "Quetzadille",
      "retweets" -> 2,
      "message" -> "RT @karolkofler: yo en el amor soy bad bunny https://t.co/BTlq7hF5Xt",
      "messageTime" -> "Thu Apr 07 00:28:10 ART 2022",
      "favorite" -> "2")

    // Assertion
    assert(
      mock.stringToJson(mockData) ==
      """{"username":"Quetzadille","message":"RT @karolkofler: yo en el amor soy bad bunny https://t.co/BTlq7hF5Xt","favorite":2,"retweets":2,"messageTime":"Thu Apr 07 00:28:10 ART 2022"}""".stripMargin
    )
  }
}