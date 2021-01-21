package top_tweets

import java.time.{LocalDateTime, ZoneId, ZonedDateTime}
import java.util.TimeZone

import MySparkSession._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType


object Helper extends Log4jSupport with Settings  {

  val baseTable: String = setBaseTable
  val analyticPath: String = setAnalyticPath
  val analyticTable: String = setAnalyticTable
  val analyticView: String = setAnalyticView
  val analyticStaging: String = setAnalyticStaging

  // setting variables for filtering landing layer data by date
  val yesterday: ZonedDateTime = ZonedDateTime.now(ZoneId.of("Europe/Amsterdam")).minusDays(1)

  val yearFilter1: Int = yesterday.getYear
  val monthFilter1: Int = yesterday.getMonthValue
  val dayFilter1: Int = yesterday.getDayOfMonth



  import spark.implicits._
  import spark.sql

  def saveStaginglTable1(newDF: DataFrame, oldDF: DataFrame): DataFrame = {


    val flatten_distinct = array_distinct _ compose flatten
    val oldDF2 = oldDF.drop("last_update")
    val newDF2 = oldDF.drop("last_update")
    val tempDF = oldDF2.union(newDF2)
/*      .withColumn("_tmp1", concat_ws(",", $"hashtag", $"rt_count"))
      .dropDuplicates("_tmp1")*/
    val tempDF2 = tempDF
      .groupBy("hashtag").agg(
      sum("rt_count") as "rt_count",
      sum("tweet_count") as "tweet_count",
      round(avg("avg_sentiment"), 2) as "avg_sentiment",
      collect_set("user_names") as "_tmp2",
      min("date_posted") as "date_posted",
      last("date_recorded") as "date_recorded",
      last("year") as "year",
      last("month") as "month",
      last("day") as "day"
      //last("_tmp1") as "_tmp2"
    ).orderBy($"rt_count".desc)
    val tempDF3 = tempDF2
      .withColumn("user_names", flatten_distinct($"_tmp2"))
      .withColumn("last_update1", dayofmonth(from_utc_timestamp(to_utc_timestamp(current_timestamp(), tz_local), tz)))
      .withColumn("last_update2", hour(from_utc_timestamp(to_utc_timestamp(current_timestamp(), tz_local), tz)))
      .select($"hashtag", $"rt_count", $"tweet_count", $"avg_sentiment", $"user_names", $"date_posted", $"date_recorded", $"last_update1", $"last_update2", $"year", $"month", $"day")
    tempDF3
  }


  def main(args: Array[String]): Unit = {

    val e = sql(s"SELECT MAX(last_update) FROM $analyticStaging").take(1)(0)
    val a = sql(s"SELECT MAX(last_update) FROM $analyticStaging").take(1)(0).getTimestamp(0).toLocalDateTime
    val b = sql(s"SELECT MAX(last_update) FROM $analyticStaging").take(1)(0).getTimestamp(0).toLocalDateTime.getDayOfMonth
    val c =  ZonedDateTime.now(ZoneId.of(tz)).getHour
    val d =  ZonedDateTime.now(ZoneId.of(tz)).getDayOfMonth

    val timezone = tz_local

    val oldDF = sql(s"SELECT  * FROM $analyticStaging")

/*    val testDF = saveStaginglTable1(oldDF, oldDF)
    testDF.show()
    testDF.printSchema()*/



    println(e)
    println(a)
    println(b)
    println(c)
    println(d)


    println(timezone)


  }

}
