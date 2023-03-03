/* -----------------------------------

Big Data
Sentiment Analysis

Topic: Artificial Intelligence
Platform: Twitter

Brian Morris
December 2022

-------------------------------------- */

-- Build tables using Athena, data located in S3 buckets

-- raw data table with scraped tweets
CREATE EXTERNAL TABLE IF NOT EXISTS `projectdb`.`tweets` (
  `id` string,
  `name` string,
  `screen_name` string,
  `tweet` string,
  `followers_count` int,
  `location` string,
  `geo` string,
  `created_at` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim' = '\t',
  'collection.delim' = '\u0002',
  'mapkey.delim' = '\u0003'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://b16-brian/dataproject/AI.csv/';


-- table from parquet with tweet sentiment, tokens, and filtered for stopwords
CREATE EXTERNAL TABLE IF NOT EXISTS `projectdb`.`sentimentFiltered` (
  `id` string,
  `name` string,
  `screen_name` string,
  `tweet` string,
  `followers_count` string,
  `location` string,  
  `geo` string,
  `created_at` string,
  `sentiment` string,
  `tokens` array < string >,
  `filtered` array < string >
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://b16-brian/dataproject/AI-sentiment-cleaned.parquet/'
TBLPROPERTIES ('classification' = 'parquet');


-- parquet table with tokens and predictions
CREATE EXTERNAL TABLE IF NOT EXISTS `projectdb`.`sentimentPredictions` (
  `id` string,
  `name` string,
  `screen_name` string,
  `tweet` string,
  `followers_count` string,
  `geo` string,
  `created_at` string,
  `sentiment` string,
  `tokens` array < string >,
  `filtered` array < string >,
  `cv` string,
  `1gram_idf` string,
  `features` string,
  `label` double,
  `rawprediction` string,
  `probability` string,
  `prediction` double
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://b16-brian/dataproject/twitter_predictions.parquet/'
TBLPROPERTIES ('classification' = 'parquet');


-- create model results table
CREATE EXTERNAL TABLE IF NOT EXISTS `projectdb`.`model_results` (
  `model_name` string,
  `accuracy` float,
  `roc_auc` float,
  `run_time` float
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = '\t')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://b16-brian/dataproject/model_results.csv/';


-- Explore the created tables

-- Explore the tweets table
desc tweets;
select * from tweets limit 5;
select count(*) from tweets;

-- Explore the sentimentFiltered table
desc sentimentFiltered;
select * from sentimentFiltered limit 5;
select count(*) from sentimentFiltered;

-- Explore the sentimentPredictions table
desc sentimentPredictions;
select * from sentimentPredictions limit 5;
select count(*) from sentimentPredictions;

-- Explore the modelResults table
desc modelResults;
select * from modelResults limit 5;
select count(*) from modelResults;



/* ------------------------
-- SENTIMENT FILTERED TABLE
-------------------------- */

-- Create a table to visualize:
-- Get the top 20 accounts by followers
create table top20followers as
select screen_name,
       max(cast(followers_count as int)) as fcount -- cast as integer, impored with parquet as string
from sentimentfiltered
group by screen_name
order by fcount desc
limit 20;


-- Create a table to visualize:
-- Get the tweets from the top 3 followed accounts, and CBC for curiousity
create table topTweets as
select screen_name,
       tweet,
       sentiment
from sentimentfiltered
where screen_name in ('BBCWorld', 'WSJ', 'Forbes', 'CBC')
limit 5;


-- Create a table to visualize:
-- get word counds from filtered tokenized tweets
CREATE TABLE sentimentFilteredWords AS
select  word, 
        count(word) as wordcount,
        sum(case when sentiment = 'positive' then 1 else 0 end) as positiveCount,
        sum(case when sentiment = 'negative' then 1 else 0 end) as negativeCount,       
        count(sentiment) as totalCount
from sentimentFiltered, unnest(filtered) as t(word)
group by word
order by wordcount desc;


-- Create sentiment count table by screen_name
create table sentimentByUser as
select  screen_name,
        sum(case when sentiment = 'positive' then 1 else 0 end) as positiveCount,
        sum(case when sentiment = 'negative' then 1 else 0 end) as negativeCount,
        count(sentiment) as sentimentCount
from sentimentFiltered
where screen_name not in ('ozgungrbz', 'JagdipSanghera') -- these two accounts are spam as found with tweet checking query below
group by screen_name
order by sentimentCount desc;

-- Helper query for above table
-- Get tweets from an individual screen_name
select screen_name, tweet
from sentimentfiltered
where screen_name = 'AISelection'
-- Found some spam accounts: 'ozgungrbz', 'JagdipSanghera'



-- Create sentiment count table by hour
create table sentimentOverTime as
-- Step 2: Extract Date, Hour, with sentiment counts, and total sentiment count
select  tweetDate,
        tweetHour,
        sum(case when sentiment = 'positive' then 1 else 0 end) as positiveCount,
        sum(case when sentiment = 'negative' then 1 else 0 end) as negativeCount,
        count(sentiment) as sentimentCount
from (
    -- Step 1: Enrich table with Date, Day, and Hour from the `created_at` field
    select  tweet,
            screen_name,
            followers_count,
            sentiment,
            hour(datetime_stamp) as tweetHour,
            day(datetime_stamp) as tweetDay,
            date(datetime_stamp) as tweetDate
    from (
        -- get date timestamp column from original `created_at` string date
        select *,
               date_parse(created_at, '%a %b %d %H:%i:%S +0000 %Y') as datetime_stamp
        from sentimentFiltered
    ) as step1
) as step2
group by tweetDate, tweetHour
order by tweetDate, tweetHour;



/* ------------------------
-- SENTIMENT PREDICTIONS TABLE
-------------------------- */

-- Create a table to visualize:
-- get word counds from filtered tokenized tweets
CREATE TABLE predictionFilteredWords AS
select word, 
       count(word) as wordcount
from sentimentPredictions, unnest(filtered) as t(word)
group by word
order by wordcount desc;



