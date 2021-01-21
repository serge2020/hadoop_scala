package top_tweets


import java.net.URL

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{ArrayType, ByteType, DateType, DoubleType, IntegerType, LongType, ShortType, StringType, StructField, StructType}
import org.scalatest.FunSuite

class BaseToAnalyticTest extends FunSuite {

  // setting test resource paths: dummy base layer table
  val file1: URL = getClass.getResource("/topTweets_base_table.orc")

  // creating Spark session to run tests
  val spark: SparkSession = SparkSession
    .builder()
    .appName("topTweetsBaseTest")
    .config("spark.master", "local[*]")
    .getOrCreate()

  // loading test dataframes from orc file
  val testDF1: DataFrame = spark.read.format("orc").load(file1.getPath)

  /** TESTS  */

  test("columns of the base layer dataframe should have datatypes according to documentation") {
    val expectedShema: StructType = StructType(Array(
      StructField("hashtag", StringType, nullable = true),
      StructField("rt_count", LongType, nullable = true),
      StructField("tweet_count", LongType, nullable = false),
      StructField("avg_sentiment", DoubleType, nullable = true),
      StructField("user_names", ArrayType(StringType, containsNull = true), nullable = true),
      StructField("date_posted",  DateType, nullable = true),
      StructField("date_recorded",  DateType, nullable = true),
      StructField("year", ShortType, nullable = true),
      StructField("month", ByteType, nullable = true),
      StructField("day", ByteType, nullable = true)))
    val actualSchema = BaseToAnalyticApp.aggregatedDF(testDF1).schema

    assert(expectedShema === actualSchema)
  }
  test("all data transformations for analytic layer before aggregation should have correct values") {
    assert(BaseToAnalyticApp.finalDF(testDF1).collect()(0).mkString(",") ===
      "RestInPeace,131,1199561037823062016,25.0,StarSportsIndia,2019-11-27,2019-11-28,2019,11,28")

  }
  test("all data aggregation for analytic layer should have correct values") {
    assert(BaseToAnalyticApp.aggregatedDF(testDF1).collect()(0).mkString(",") ===
      "IGOT7,2788,2,25.0,WrappedArray(GOT7_Japan, GOT7Official),2019-11-28,2019-11-28,2019,11,28")

  }
}
