package top_tweets

import java.time.{ZoneId, ZonedDateTime}

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType}
import org.apache.spark.sql.DataFrame


import MySparkSession._

/** standalone app for loading application data from landing layer, extracting the necessary
 * fileds from laning hive table and saving it into base layer Hive table */
object LandToBaseApp extends Log4jSupport with Settings {

  val landingTable: String = setLandingTable
  val basePath: String = setBasePath
  val baseTable: String = setBaseTable

  // setting variables for filtering landing layer data by date
  val yesterday: ZonedDateTime = ZonedDateTime.now(ZoneId.of(tz)).minusDays(daysBack1)
  val yearFilter: Int = yesterday.getYear
  val monthFilter: Int = yesterday.getMonthValue
  val dayFilter: Int = yesterday.getDayOfMonth

  import spark.implicits._
  import spark.sql

  /** function that retrieves fields from 'full_tweet' column of landing layer Hive table according to
   * base layer table schema specification and adds them as columns to output dataframe. It retrieves hashtags as a string array from
   * tweet record 'entities.hashtags' JSON array, text of the tweet from extended_tweet.full_text
   * if it is present or from text field if the former is absent. Year, month and day columns are added from the landing table.   *
   * @param df input dataframe created from landing layer Hive table
   * @return dataframe that is ready to be saved into base layer Hive table   */
  def transformToBase(df: DataFrame): DataFrame = {
    val createdDF1 = df
      .withColumn("_tmp1", get_json_object(col("full_tweet"), "$.retweeted_status.extended_tweet.entities.hashtags"))
      .withColumn("_tmp2", get_json_object(col("full_tweet"), "$.retweeted_status.entities.hashtags"))
      .withColumn("_tmp3", coalesce($"_tmp1", $"_tmp2"))
      .withColumn("_tmp4", get_json_object(col("_tmp3"), "$[*].text"))
      .withColumn("_tmp5", $"_tmp4" cast StringType)
      .withColumn("_tmp6", regexp_replace(col("_tmp5"), "\"", ""))
      .withColumn("_tmp7", regexp_replace(col("_tmp6"), "\\s", ""))
      .withColumn("_tmp8", regexp_replace(col("_tmp7"), ",", " "))
      .withColumn("_tmp9", regexp_replace(col("_tmp8"), "[\\[\\]]", ""))
      .withColumn("_tmp10", regexp_replace($"_tmp9", "([^\\w\\s]+)", ""))
      .withColumn("_tmp11", split($"_tmp10", " "))
      .withColumn("_tmp12", array_remove($"_tmp11", ""))
      .withColumn("_tmp13", array_remove($"_tmp12", "\\s"))
      .withColumn("hashtags", array_distinct($"_tmp13"))
      .withColumn("_tmp15", get_json_object($"full_tweet", "$.retweeted_status.text"))
      .withColumn("_tmp16", get_json_object($"full_tweet", "$.retweeted_status.extended_tweet.full_text"))
      .withColumn("_tmp17", coalesce($"_tmp16", $"_tmp15"))
      .withColumn("_tmp18", regexp_replace($"_tmp17", "^(https?|ftp|file):\\/\\/[-a-zA-Z0-9+&@#\\/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#\\/%=~_|]", ""))  // negative match for all urls
      .withColumn("_tmp19", regexp_replace($"_tmp18", "[^\\u0000-\\uFFFF]", "")) // negative match for emojis
      .withColumn("_tmp20", regexp_replace($"_tmp19", "([^\\w\\s]+)", "")) // negative match for words containing only alphanumeric characters
      .withColumn("_tmp21", regexp_replace($"_tmp20", "\\b(http.*)", "")) // match any urls still left
      .withColumn("tweet_txt", regexp_replace($"_tmp21", "\n", "")) // match for line breaks
      .withColumn("created", get_json_object($"full_tweet", "$.retweeted_status.created_at"))
      .withColumn("tweet_id", get_json_object($"full_tweet", "$.retweeted_status.id_str"))
      .withColumn("rt_count", get_json_object($"full_tweet", "$.retweeted_status.retweet_count").cast(LongType))
      .withColumn("user_id", get_json_object($"full_tweet", "$.retweeted_status.user.id_str"))
      .withColumn("user_name", get_json_object($"full_tweet", "$.retweeted_status.user.screen_name"))
      .select("created", "tweet_id", "tweet_txt", "hashtags", "rt_count", "user_id", "user_name", "year", "month", "day").na.drop()
    createdDF1
  }

  def main(args: Array[String]): Unit = {

    // loading data from the landing layer as Spark dataframe
    val landingDF: DataFrame = sql(s"SELECT * FROM $landingTable WHERE year >= $yearFilter AND month >= $monthFilter AND day >= $dayFilter")

    // applying transformations for base layer
    val baseFD: DataFrame = transformToBase(landingDF)
    val recCount: Long = baseFD.count()
    // save the resulting dataframe into base layer Hive table
    baseFD
      .write
      .partitionBy("year", "month", "day")
      .format("hive")
      .mode("append")
      .option("path", basePath)
      .saveAsTable(baseTable)

    //baseFD.show()
    //baseFD.printSchema()
    println(s" $recCount tweet records saved to base layer hive table")

  }
}
