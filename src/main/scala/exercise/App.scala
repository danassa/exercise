package exercise

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import exercise.Constants._
import org.apache.spark.sql.functions.{collect_set, countDistinct}


//you are requested to compute a daily usage statistics computation of the users of our customers. You need to build a system that will do a daily batch processing, that will allow data analysts to query these usage statistics, and draw some insights.
//The input data is clickstream information that is kept in some file system like S3 or HDFS, in files that contain a bunch of JSON documents in them. Each document represents 1 event. Each such file represents 1 minute of data.
//
//The statistics we want to compute for each customer (client_id) are:
//● What is the number of activities used per user and per account in the last 1, 3, 7, 14, 30, 90, 180, 365 days
//● What is the number of modules used per user and per account in the last 1, 3, 7, 14, 30, 90, 180, 365 days
//● What is the number of unique users per account in the last 1, 3, 7, 14, 30, 90, 180, 365 days
//
//Questions
//1. What architecture do you suggest that will support those requirements?
//How will you schedule this computation? How will you do the actual computation? Where will you keep the results? Will your answer change if you need to query in realtime or in an analytics dashboard? How will you make sure the system can handle up to 1 year of data in a timely manner?
//2. How is the Activity and Module Aggregation calculated?
//Write a spark program that shows that.
//3. How is the number of unique users calculated?

object App {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("intuit-exercise").setMaster("local[*]")
    implicit val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val raw = read(args(0))

    val grouped = collectActivitiesAndModules(raw)
    grouped.persist()
    val result = collectUsers(grouped)

    grouped.show(10, false)
    result.show(10, false)

//    result.write.parquet(args(1))
//    grouped.write.parquet(args(2))

    spark.close()
  }

  def read(path: String)(implicit spark:SparkSession): DataFrame ={
    spark.read.json(path)
  }

  // 2. How is the Activity and Module Aggregation calculated?
  def collectActivitiesAndModules(df: DataFrame): DataFrame = {
    df.groupBy(DATE, CLIENT_ID, ACCOUNT_ID, USER_ID)
      .agg(
        collect_set(ACTIVITY).alias(ACTIVITIES),
        collect_set(MODULE).alias(MODULES)
      )
  }

  //3. How is the number of unique users calculated?
  def collectUsers(df: DataFrame): DataFrame = {
    df.select(DATE, CLIENT_ID, ACCOUNT_ID, USER_ID)
      .groupBy(DATE, CLIENT_ID, ACCOUNT_ID)
      .agg(
        collect_set(USER_ID).alias(USERS)
      )
  }


}
