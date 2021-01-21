package top_tweets
/**
 * @author ${sergejs.nesterovs}
 *
 *   "top_tweets" application Provides access to Twitter streaming API utilising twitter.hbc.core library
 *   It provides functuionality to apply filters on "lang", "retweeted_status" fields of the tweet record
 *   and retrieve the "retweet_count" value. It filters Twitter stream returning tweets as JsonObject types.
 */
import java.util.concurrent.LinkedBlockingQueue

import com.twitter.hbc.core.Client
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint
import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.{Constants, HttpHosts}
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.httpclient.auth.OAuth1

import scala.util.{Failure, Success, Try}
import com.google.gson.{JsonObject, JsonParser}

object ProducerApp extends Settings {

  // Setting twitter params source file
  // loading filter params  and  number of tweets to collect
  val language: String = setLanguage
  val minRetweets: Int = setMinRetweets
  val numTweetsToCollect: Int = setNumTweetsToCollect
  var numTweets = 0

  val oAuth1 = new OAuth1(
    setConsumerKey,
    setConsumerSecret,
    setToken,
    setTokenSecret)

  //connecting to Twitter streaming API
  val msgQueue = new LinkedBlockingQueue[String](setQueLength)
  val hosebirdHosts = new HttpHosts(Constants.STREAM_HOST)
  val hosebirdEndpoint = new StatusesSampleEndpoint()

  val simpleBuilder: ClientBuilder = new ClientBuilder()
    .hosts(hosebirdHosts)
    .authentication(oAuth1)
    .endpoint(hosebirdEndpoint)
    .processor(new StringDelimitedProcessor(msgQueue))

  /** FILTERING FUNCTIONS */
  /*** @param jsonObj input tweet record casted as JsonObject
   *   @param lang  language to apply for filtering using Twitter API acronyms (English = "en")
   *   @return the language string if  it matches specified language, "other" string if tweet is in another language
   *         or "null" string if field value is null
   */
  def getLang(jsonObj: JsonObject, lang: String ): String = {
    Try(jsonObj.get("lang").getAsString)  match {
      case Success(s) if s == lang => s
      case Failure(e) => "null"
      case _ => "other"
    }
  }
  /*** @param jsonObj input tweet record casted as JsonObject
   *   @return "yes" string if "retweeted_status" field is not null or "no" string if it is null
   */
  def getRtStatus(jsonObj: JsonObject): String = {
    Try(jsonObj.get("retweeted_status")) match {
      case Success(s) if s != null => "yes"
      case Failure(e) => "no"
      case _ => "no"
    }
  }
  /*** @param jsonObj input tweet record casted as JsonObject
   *   @return value (Int) of "retweet_count" field within the aggregated "retweeted_status" field
   *         returns -1 if "retweet_count" is null or doesn't exist
   */
  def getRtCount(jsonObj: JsonObject): BigInt = {
    val rt = getRtStatus(jsonObj)
    rt match {
      case "no" => -1
      case _ =>
        jsonObj
          .get("retweeted_status")
          .getAsJsonObject
          .get("retweet_count").getAsBigInteger
    }
  }
  /** function that filters out tweets with no hashtags. It takes
   * @param jsonObj  of JsonObject type  - full tweet record, parses it and
   * @return Boolean true if hashtags are present or false if they are not
   */
  def getHashtags(jsonObj: JsonObject): Boolean = {
    Try(jsonObj.get("retweeted_status").getAsJsonObject.get("extended_tweet").getAsJsonObject) match {
      case Failure(e) =>
        Try(jsonObj.get("retweeted_status").getAsJsonObject) match {
          case Failure(e) => false
          case Success(i) =>
            val hashTg = i
              .get("entities").getAsJsonObject
              .get("hashtags").getAsJsonArray
            if (hashTg.size() > 0) true
            else false
        }
      case Success(j) =>
        val hashTg = j
          .get("entities").getAsJsonObject
          .get("hashtags").getAsJsonArray
        if (hashTg.size() > 0) true
        else false
    }
  }

  def main(args: Array[String]) {

    // connect to Twitter http API and apply filters
    val simpleHosebirdClient: Client = simpleBuilder.build()
    simpleHosebirdClient.connect()
    println("connecting to Twitter https...")
    val producerOne = new TopTweetsProducer
    println("launching Kafka producer...")

    // loop is running while the necessary amount of tweets is collected
    while (!simpleHosebirdClient.isDone) {

      Try (msgQueue.take()) match {
        case Success(i) =>
          //  parsing twitter stream int Json object
          val jsonObject = new JsonParser().parse(i).getAsJsonObject

          // selecting only tweets that pass the filters
          if (getLang(jsonObject, language) == language && getRtStatus(jsonObject) == "yes" && getRtCount(jsonObject) >= minRetweets && getHashtags(jsonObject)) {
            numTweets += 1
            /** Zookeeper server, broker server and KafkaConsumer to console or sink need to be open
             * before running this app */
            val tweetToKafka = jsonObject.toString
            producerOne.sendRecord(tweetToKafka)
            // CLOSE CONNECTION WHEN THE REQUIRED NUMBER OF TWEETS IS COLLECTED
            if (numTweets >= numTweetsToCollect.toString.toInt) {
              println(s"$numTweets tweets collected, closing http connection...")
              Thread.sleep(3000)
              simpleHosebirdClient.stop()
              producerOne.flush()
              producerOne.close()
            }
          }
        // if  msgQue element is empty or throws an error ' while' loop proceeds to the next element
        case Failure(e) => e.printStackTrace()
      }
    }
  }
}
