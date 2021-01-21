package top_tweets

import java.time.{LocalDateTime, ZoneId, ZonedDateTime}
import java.util. Properties

import collection.JavaConverters._
import collection.mutable._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import edu.stanford.nlp.util.CoreMap
import edu.stanford.nlp.pipeline._
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import MySparkSession._

import scala.util.{Failure, Success, Try}

/** standalone app for loading application data from base layer, applying the necessary
 * transformations and saving it into analytic layer Hive table */
object BaseToAnalyticApp extends Log4jSupport with Settings {

  val baseTable: String = setBaseTable
  val analyticPath: String = setAnalyticPath
  val analyticTable: String = setAnalyticTable
  val analyticView: String = setAnalyticView
  val analyticStaging: String = setAnalyticStaging

  // setting variables for filtering landing layer data by date
  val yesterday: ZonedDateTime = ZonedDateTime.now(ZoneId.of(tz)).minusDays(daysBack1)

  val yearFilter1: Int = yesterday.getYear
  val monthFilter1: Int = yesterday.getMonthValue
  val dayFilter1: Int = yesterday.getDayOfMonth

  import spark.implicits._
  import spark.sql

  /** function that applies the necessary transformations to base layer table which is loaded to
    * @param df dataframe. To avoid multiple joins it maps over dataframe rows calculating sentiment score
   *           for each tweet 'text_txt' record using Stanford NLP library, parses 'created' to date datatype and
   * @return dataframe with columns necessary for final aggreagtion.  */
  def finalDF(df : DataFrame): DataFrame = {
    val tempDF1 = df
      .dropDuplicates("tweet_id")
      .map(row => {
        val created = row.getString(0)
        val tweet_id = row.getString(1)
        val tweet_txt = row.getString(2)
        val hashtags = row.getAs[Seq[String]](3)
        val rt_count = row.getLong(4)
        val user_id = row.getString(5)
        val user_name = row.getString(6)
        val year = row.getShort(7)
        val month = row.getByte(8)
        val day = row.getByte(9)
        val sentiment: Double = {
          val props = new Properties()
          props.setProperty("annotators", "tokenize, ssplit, parse, sentiment") // selecting NLP pipeline annotators
          // retrieving annotated sentences to Scala mutable list (analogue of java.util.List)
          val sentenceMap: scala.collection.mutable.Buffer[CoreMap] = new StanfordCoreNLP(props).process(tweet_txt).get(classOf[CoreAnnotations.SentencesAnnotation]).asScala
          val sentimentList =  for (i <- sentenceMap.indices) yield
            // calculating sentiment scores for tweet sentences and their average
            RNNCoreAnnotations.getPredictedClass(sentenceMap(i).get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree]))
          val score = Try(sentimentList.sum/sentimentList.size)  match {
            case Success(i) => i
            case Failure(e) => 2
          }
          score
        }
        (tweet_id, hashtags, rt_count, user_id, user_name, sentiment, created, year, month, day)
      }).toDF("tweet_id", "hashtags", "rt_count", "user_id", "user_name", "sentiment", "created", "year", "month", "day")
      .withColumn("date_posted", to_date($"created", "EEE MMM dd HH:mm:ss Z yyyy"))
      .withColumn("sentiment_%", $"sentiment" / 4 * 100)
      .withColumn("_tmp1", concat_ws("-", $"year", $"month", $"day"))
      .withColumn("date_recorded", to_date($"_tmp1", "yyyy-MM-dd"))
      .withColumn("hashtag", explode($"hashtags"))
      .select($"hashtag", $"rt_count", $"tweet_id", $"sentiment_%", $"user_name", $"date_posted", $"date_recorded",  $"year", $"month", $"day")
    tempDF1
  }

  /**  function that applies the final aggregation grouping by 'hashtag' column. It takes dataframe created by 'finalDF' function as input
   * @param df aggregates columns 'rt_count' (sum), 'tweet_id' (count), 'sentiment' (avg), 'user_name' (collect_set), 'date' (min) and
   * @return the final dataframe with aggregated data for each unique hashtag.    */
  def aggregatedDF(df: DataFrame): DataFrame = {
    val tempDF1 = finalDF(df)
    val tempDF2 = tempDF1.groupBy("hashtag").agg(
      sum("rt_count") as "rt_count",
      count("tweet_id") as "tweet_count",
      round(avg("sentiment_%"), 2) as "avg_sentiment",
      collect_set("user_name") as "user_names",
      min("date_posted") as "date_posted",
      first("date_recorded") as "date_recorded",
      first("year") as "year",
      first("month") as "month",
      first("day") as "day"
    ).orderBy($"rt_count".desc)
    tempDF2
  }

  /** function that takes no input parameters, retrieves the latest record date fromAna;ytic layer staging table and
   * @return Boolean 'true' if there have been records saved today (or the table iz empty) or 'false' if last record
   * from the table is older than as of today. */
  def updatedToday: Boolean = {

    val current = ZonedDateTime.now(ZoneId.of(tz)).getDayOfMonth
    val last = Try(sql(s"SELECT MAX(last_update) FROM $analyticStaging").take(1)(0).getTimestamp(0).toLocalDateTime.getDayOfMonth) match {
      case Success(i) => i
      case Failure(e) => 0
    }
    val updated =  if (last == 0 || last == current) true else false
    updated
  }

  /** function that deduplicates records from  top_tweets_staging_test table. It takes two dataframes as input parameters:
   * @param newDF last day's data loaded from Base layer table with transformations applied,
   * @param oldDF records that are already saved to Analytic layer staging table. The function
   * @return dataframe that contains only unique records from both input dataframes with 'last_update' column containing timestamp of its creation   */
  def saveStaginglTable1(newDF: DataFrame, oldDF: DataFrame): DataFrame = {

    val flatten_distinct = array_distinct _ compose flatten
    val oldDF2 = oldDF.drop("last_update")
    val tempDF = oldDF2.union(newDF)
      .withColumn("_tmp1", concat_ws(",", $"hashtag", $"rt_count"))
      .dropDuplicates("_tmp1")
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
      last("day") as "day",
      last("_tmp1") as "_tmp3"
    ).orderBy($"rt_count".desc)
    val tempDF3 = tempDF2
      .withColumn("user_names", flatten_distinct($"_tmp2"))
      .withColumn("last_update", from_utc_timestamp(to_utc_timestamp(current_timestamp(), tz_local), tz))
      .select($"hashtag", $"rt_count", $"tweet_count", $"avg_sentiment", $"user_names", $"date_posted", $"date_recorded", $"last_update", $"year", $"month", $"day")
    tempDF3
  }

  /** function that deduplicates records from dataframe created by 'aggregatedDF' function. It takes
   * @param df - dataframe as input parameter and
   * @return dataframe that contains only unique records with 'last_update' column containing timestamp of its creation   */
  def saveStaginglTable2(df: DataFrame): DataFrame = {

    val tempDF = df
      .withColumn("_tmp1", concat_ws(",", $"hashtag", $"rt_count"))
      .dropDuplicates("_tmp1")
      .withColumn("last_update", from_utc_timestamp(to_utc_timestamp(current_timestamp(), tz_local), tz))
      .select($"hashtag", $"rt_count", $"tweet_count", $"avg_sentiment", $"user_names", $"date_posted", $"date_recorded", $"last_update", $"year", $"month", $"day")
    tempDF
  }

  /** function that saves records from  staging to final Analytic layer table. It takes input
   * @param df - dataframe containing records from top_tweets_staging_test table and
   * @return dataframe ready to be saved to Analytic layer final table.   */
  def saveFinalTable(df: DataFrame): DataFrame = {

    val tempDF =  df
      .withColumn("rank",row_number().over(Window.partitionBy($"date_recorded").orderBy($"rt_count".desc)))
      .withColumn("_tmp1", date_format($"date_recorded", "yyyyMMdd"))
      .withColumn("hashtag_id", concat_ws("-", $"_tmp1", $"rank"))
      .select($"hashtag_id", $"hashtag", $"rt_count", $"tweet_count", $"avg_sentiment", $"user_names", $"date_posted", $"date_recorded", $"year", $"month", $"day")
    tempDF
  }

  def main(args: Array[String]): Unit = {

    // loading data from the landing layer as Spark dataframe
    val baseDF: DataFrame = sql(s"SELECT * FROM $baseTable WHERE year >= $yearFilter1 AND month >= $monthFilter1 AND day >= $dayFilter1")

    // applying transformations for analytical layer
    val newDF: DataFrame = aggregatedDF(baseDF)

    if (updatedToday) {
      val oldDF = sql(s"SELECT  * FROM $analyticStaging")
      val recCount1: Long = oldDF.count()
      val tempTable = saveStaginglTable1(newDF, oldDF)
      val recCount2: Long = tempTable.count()
      tempTable
        .write
        .format("hive")
        .mode("overwrite")
        .saveAsTable("temp_table")
      val stagingTable = sql("SELECT * FROM temp_table")
      stagingTable
        .write
        .format("hive")
        .mode("overwrite")
        .saveAsTable(analyticStaging)
      println(s" ${recCount2 - recCount1} new  tweet records saved to analytic layer staging table.")
    }
    else {
     val stagingDF = sql(s"SELECT * FROM $analyticStaging")
      val finalTable = saveFinalTable(stagingDF)
      finalTable
        .write
        .partitionBy("year", "month", "day")
        .format("hive")
        .mode("append")
        .option("path", analyticPath)
        .saveAsTable(analyticTable)

      val recCount: Long = finalTable.count()
      sql(s"TRUNCATE TABLE $analyticStaging")

      val tempTable = saveStaginglTable2(newDF)
      val recCount2: Long = tempTable.count()
      tempTable
        .write
        .format("hive")
        .mode("overwrite")
        .saveAsTable(analyticStaging)
      println(s" $recCount new  tweet records saved to analytic layer final table")
      println(s" $recCount2 tweet records saved to analytic layer staging table")
    }
    //sql(s"SELECT * FROM  $analyticView").show()
  }
}
