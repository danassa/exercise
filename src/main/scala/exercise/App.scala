package exercise

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import exercise.Constants._
import org.apache.spark.sql.functions.{collect_set, countDistinct}


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

  // 3. How is the number of unique users calculated?
  def collectUsers(df: DataFrame): DataFrame = {
    df.select(DATE, CLIENT_ID, ACCOUNT_ID, USER_ID)
      .groupBy(DATE, CLIENT_ID, ACCOUNT_ID)
      .agg(
        collect_set(USER_ID).alias(USERS)
      )
  }

  def countActivitiesAndModules(df: DataFrame): DataFrame = {
    df.groupBy(DATE, CLIENT_ID, ACCOUNT_ID, USER_ID)
      .agg(
        countDistinct(ACTIVITY).alias(ACTIVITIES),
        countDistinct(MODULE).alias(MODULES)
      )
  }

  // 3. How is the number of unique users calculated?
  def counttUsers(df: DataFrame): DataFrame = {
    df.select(DATE, CLIENT_ID, ACCOUNT_ID, USER_ID)
      .groupBy(DATE, CLIENT_ID, ACCOUNT_ID)
      .agg(
        countDistinct(USER_ID).alias(USERS)
      )
  }

}
