package utils_for_tests

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.scalatest.Suite
import scala.util.Try

/**
  * Trait to be extended by unit tests for Spark applications.
  * It initialize a small spark session that can be accessed within the extending class,
  * and provides methods to compare Datasets, DataFrames and RDDs.
  *
  * To be used instead of "holdenk/spark-testing-base" package when:
  *   a. it's not up-to-date with our latest Spark version, or
  *   b. we want to ignore rows order or fields nullability
  */
trait SparkForTests { self: Suite =>
  
  val conf: SparkConf = new SparkConf()
    .setMaster("local")
    .setAppName("Spark Unit Tests")
    .set("spark.sql.shuffle.partitions", "4")
  
  /**
    * override this function if you need a custom spark session
    */
  def extendConf(conf: SparkConf): SparkConf = conf
  
  val spark: SparkSession = SparkSession.builder.config(extendConf(conf)).getOrCreate
  spark.sparkContext.setLogLevel("ERROR")
  
  /**
    * Compare two DataFrames
    * @param actual the result of the unit test computation
    * @param expected the expected result of said computation
    * @param compareSchema whether we would like to compare the actual schema or just compare the column names.
    *                      should be set to false when we would like to ignore fields nullability. defaults to true
    * @param outputToConsole whether we would like to print the schemas and some of the data to the console.
    *                        defaults to false
    */
  def assertDataFramesEquality(actual: DataFrame,
                               expected: DataFrame,
                               compareSchema: Boolean = true,
                               outputToConsole: Boolean = false): Unit = {
    failGracefully(() =>
      ComparisonTools.assertDataFramesEquality(actual, expected, compareSchema, outputToConsole)
    )
  }
  
  // just a nice to have workaround -
  // failing the test with a scalatest failure, print error to console only once
  private def failGracefully(body: () => Unit): Unit = {
    Try(
      body.apply()
    ).failed.foreach(exception => fail(exception))
  }
  
}
