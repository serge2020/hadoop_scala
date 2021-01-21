package top_tweets

import java.net.URL
import java.io.File
import com.google.gson.{JsonObject, JsonParser}
import scala.io.BufferedSource
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.FunSuite

class TopTweetsTest extends FunSuite {

  // setting test resource paths: dummy tweet files and config file
  val file1: URL = getClass.getResource("/raw_tweet_filtr.json")
  val file2: URL = getClass.getResource("/raw_tweet_noEN.json")
  val file3: URL = getClass.getResource("/raw_tweet_noRT.json")
  val file4: URL = getClass.getResource("/isRT_Tweet.json")
  val conFile: URL = getClass.getResource( "/top_tweets_test.conf")

  // load the language string used in filtering functions
  val config: Config = ConfigFactory.parseFile(new File(conFile.getPath))
  val tConfig: Config = config.getConfig("twitter")
  val language: String = tConfig.getString("language")


  // function that reads dummy tweet to Json object
  def getJsonObject(file: URL): JsonObject = {
    val jsonFile: BufferedSource = scala.io.Source.fromURL(file)
    val jsonObj = new JsonParser().parse(jsonFile.mkString).getAsJsonObject
    jsonFile.close()
    jsonObj
  }
  // create Json objects as dummy inputs for testing filters
  val tweetFiltered: JsonObject = getJsonObject(file1)
  val tweetNoLanguage: JsonObject = getJsonObject(file2)
  val tweetNoRetweets: JsonObject = getJsonObject(file3)
  val tweetWithHashtags: JsonObject = getJsonObject(file4)

  /** TESTS  */
  test("Languages other than English should be filtered out") {
    assert(ProducerApp.getLang(tweetFiltered, language) === language)
    assert(ProducerApp.getLang(tweetNoLanguage, language) === "null")
  }
  test("Tweets that are not retweets should be filtered out") {
    assert(ProducerApp.getRtStatus(tweetFiltered) == "yes")
    assert(ProducerApp.getRtStatus(tweetNoRetweets) == "no")
  }
  test("Getting the number of retweets from the tweet without retweets should not throw an error") {
    assert(ProducerApp.getRtCount(tweetNoRetweets) == -1)
  }
  test("The number of retweets returned should be correct") {
    assert(ProducerApp.getRtCount(tweetFiltered) == 136)
    assert(ProducerApp.getRtCount(tweetNoLanguage) == 143)
  }
  test("Hashtag filter works properly") {
    assert(ProducerApp.getHashtags(tweetWithHashtags) )
    assert(!ProducerApp.getHashtags(tweetFiltered))
  }
}


