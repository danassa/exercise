package exercise

import java.time.LocalDate
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import exercise.Constants._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.{col, collect_set, countDistinct}


object App {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("intuit-exercise").setMaster("local[*]")
    implicit val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val earliest = spark.sparkContext.broadcast(getDate(args(0).toInt))
    val latest = spark.sparkContext.broadcast(getDate(1))

    val raw = read(args(1), earliest, latest)
    raw.persist()

    val grouped = countActivitiesAndModules(raw)
    val result = countUsers(raw)

    grouped.show(10, false)
    result.show(10, false)

    spark.close()
  }

  def read(path: String, earliest: Broadcast[LocalDate], latest: Broadcast[LocalDate])(implicit spark:SparkSession): DataFrame ={
    spark.read.json(path)
      .filter(col(DATE) >= earliest.value && col(DATE) <= latest.value)
  }

  def countActivitiesAndModules(df: DataFrame): DataFrame = {
    df.groupBy(CLIENT_ID, ACCOUNT_ID, USER_ID)
      .agg(
        countDistinct(ACTIVITY).alias(ACTIVITIES),
        countDistinct(MODULE).alias(MODULES)
      )
  }

  def countUsers(df: DataFrame): DataFrame = {
    df.groupBy(CLIENT_ID, ACCOUNT_ID)
      .agg(
        countDistinct(USER_ID).alias(USERS)
      )
  }

  def getDate(minusDays: Int = 0): LocalDate = {
    LocalDate.now().minusDays(minusDays)
  }


//  def collectActivitiesAndModules(df: DataFrame): DataFrame = {
//    df.groupBy(DATE, CLIENT_ID, ACCOUNT_ID, USER_ID)
//      .agg(
//        collect_set(ACTIVITY).alias(ACTIVITIES),
//        collect_set(MODULE).alias(MODULES)
//      )
//  }
//
//  def collectUsers(df: DataFrame): DataFrame = {
//    df.select(DATE, CLIENT_ID, ACCOUNT_ID, USER_ID)
//      .groupBy(DATE, CLIENT_ID, ACCOUNT_ID)
//      .agg(
//        collect_set(USER_ID).alias(USERS)
//      )
//  }

}
