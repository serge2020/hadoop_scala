-- Enable Hive dynamic partitioning

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;


-- Create landing layer tables for top_tweets data asset

CREATE EXTERNAL TABLE IF NOT EXISTS tweeter_landing_test.top_tweets_test (topic STRING, time_id BIGINT, full_tweet STRING)
  PARTITIONED BY (year SMALLINT, month TINYINT, day TINYINT)
  STORED AS ORC
  LOCATION '/interns/test/landing/tweeter_landing_test/top_tweets_test';

CREATE EXTERNAL TABLE IF NOT EXISTS tweeter_landing.top_tweets (topic STRING, time_id BIGINT, full_tweet STRING)
  PARTITIONED BY (year SMALLINT, month TINYINT, day TINYINT)
  STORED AS ORC
  LOCATION '/interns/prod/landing/tweeter_landing/top_tweets';

-- Create base layer tables for top_tweets data asset

CREATE EXTERNAL TABLE IF NOT EXISTS tweeter_base_test.top_tweets_test
  (created STRING, tweet_id STRING, tweet_txt STRING, hashtags  ARRAY<STRING>, rt_count BIGINT, user_id STRING, user_name STRING)
  PARTITIONED BY (year SMALLINT, month TINYINT, day TINYINT)
  STORED AS ORC
  LOCATION '/interns/test/base/tweeter_base_test/top_tweets_test';

CREATE EXTERNAL TABLE IF NOT EXISTS tweeter_base.top_tweets
(created STRING, tweet_id STRING, tweet_txt STRING, hashtags  ARRAY<STRING>, rt_count BIGINT, user_id STRING, user_name STRING)
 PARTITIONED BY (year SMALLINT, month TINYINT, day TINYINT)
 STORED AS ORC
 LOCATION '/interns/prod/base/tweeter_base/top_tweets';


-- Create analytical layer tables and views for top_tweets data asset

 CREATE EXTERNAL TABLE IF NOT EXISTS tweeter_analytical_test.top_tweets_test
  (hashtag_id STRING, hashtag STRING, rt_count BIGINT, tweet_count BIGINT, avg_sentiment DOUBLE, user_names ARRAY<STRING>, date_posted DATE, date_recorded DATE)
  PARTITIONED BY (year SMALLINT, month TINYINT, day TINYINT)
  STORED AS ORC
  LOCATION '/interns/test/analytical/tweeter_analytical_test/top_tweets_test';

CREATE TABLE IF NOT EXISTS tweeter_analytical_test.top_tweets_staging_test
 (hashtag STRING, rt_count BIGINT, tweet_count BIGINT, avg_sentiment DOUBLE, user_names ARRAY<STRING>, date_posted DATE, date_recorded DATE, last_update DATE, year SMALLINT, month TINYINT, day TINYINT)
 STORED AS ORC
 LOCATION '/user/hive/warehouse';

 CREATE VIEW IF NOT EXISTS tweeter_analytical_test.top10_tweets_view_test
  AS SELECT * FROM
  (SELECT date_recorded, rank() over (partition by date_recorded order by  rt_count desc) AS rank, hashtag, rt_count, avg_sentiment, date_posted from tweeter_analytical_test.top_tweets_test)
  t WHERE rank < 11;

 CREATE EXTERNAL TABLE IF NOT EXISTS tweeter_analytical.top_tweets
   (hashtag_id STRING, hashtag STRING, rt_count BIGINT, tweet_count BIGINT, avg_sentiment DOUBLE, user_names ARRAY<STRING>, date_posted DATE, date_recorded DATE)
  PARTITIONED BY (year SMALLINT, month TINYINT, day TINYINT)
  STORED AS ORC
 LOCATION '/interns/prod/analytical/tweeter_analytical/top_tweets';

 CREATE TABLE IF NOT EXISTS tweeter_analytical.top_tweets_staging
  (hashtag STRING, rt_count BIGINT, tweet_count BIGINT, avg_sentiment DOUBLE, user_names ARRAY<STRING>, date_posted DATE, date_recorded DATE, last_update DATE, year SMALLINT, month TINYINT, day TINYINT)
  STORED AS ORC
  LOCATION '/user/hive/warehouse';

CREATE VIEW IF NOT EXISTS tweeter_analytical.top10_tweets_view
 AS SELECT * FROM
  (SELECT date_recorded, rank() over (partition by date_recorded order by  rt_count desc) AS rank, hashtag, rt_count, avg_sentiment, date_posted from tweeter_analytical.top_tweets)
 t WHERE rank < 11;