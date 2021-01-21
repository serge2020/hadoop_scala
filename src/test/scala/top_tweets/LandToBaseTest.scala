package top_tweets

import java.net.URL

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, StringType, StructField, StructType}
import org.scalatest.FunSuite

class LandToBaseTest extends FunSuite {

  // setting test resource paths: dummy tweet file
  val file1: URL = getClass.getResource("/tweets300.orc")

  // creating Spark session to run tests
  val spark: SparkSession = SparkSession
    .builder()
    .appName("topTweetsBaseTest")
    .config("spark.master", "local[*]")
    .getOrCreate()


  // loading test dataframes from orc files
  val testDF1: DataFrame = spark.read.format("orc").load(file1.getPath)


  /** TESTS  */

  test("columns of the base layer dataframe should have datatypes according to documentation") {
    val expectedShema: StructType = StructType(Array(
      StructField("created", StringType,nullable = true),
      StructField("tweet_id", StringType,nullable = true),
      StructField("tweet_txt", StringType,nullable = true),
      StructField("hashtags", ArrayType(StringType, containsNull = true), nullable = true),
      StructField("rt_count", LongType, nullable = true),
      StructField("user_id", StringType, nullable = true),
      StructField("user_name", StringType, nullable = true),
      StructField("year", IntegerType, nullable = true),
      StructField("month",IntegerType,nullable = true),
      StructField("day",IntegerType,nullable = true)))
    val actualSchema = LandToBaseApp.transformToBase(testDF1).schema

    assert(expectedShema === actualSchema)
  }


  test("all data transformations for base layer should have correct values") {
    assert(LandToBaseApp.transformToBase(testDF1).collect()(0).mkString(",") ===
      "Wed Nov 27 09:24:39 +0000 2019,1199620036299608064,So Seokjin originally did the adlibs in head voice but they changed it to mixed in the final version I think they were talking about chest voice and head voice at the end King of musicianship     JIN BTSJIN BTS_twt ,WrappedArray(JIN, BTSJIN),207,835955056981626880,JessicaJungxBgA,2019,11,28")
  }

}


