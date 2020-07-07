package exercise

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite
import utils_for_tests.SparkForTests
import Constants._
import scala.io.Source


class AppTest extends FunSuite with SparkForTests {
  import spark.implicits._

  val date7th: LocalDate = LocalDate.parse("2020-07-07", DateTimeFormatter.ofPattern("yyyy-MM-dd"))
  val date5th = date7th.minusDays(2)

  test("test read one day only") {
    val input: String = Source.getClass.getResource("/lake").getPath + "/"

    val dt = spark.sparkContext.broadcast(date7th)
    var actualResult = App.read(input, dt, dt)(spark)

    val expectedResult: DataFrame = Seq(
      (date7th, 1234, "john@foo.com", 4567, "activity_1", "module_1"),
      (date7th, 1234, "john@foo.com", 9876, "activity_1", "module_1"),
      (date7th, 1234, "doe@bar.com",  4567, "activity_1", "module_1")
    ).toDF(DATE, CLIENT_ID, USER_ID, ACCOUNT_ID, ACTIVITY, MODULE)
    assertDataFramesEquality(actualResult, expectedResult, false)
  }

  test("test read 3 days") {
    val input: String = Source.getClass.getResource("/lake").getPath + "/"

    val dt = spark.sparkContext.broadcast(date7th)
    val dt_minus_3_days = spark.sparkContext.broadcast(date5th)
    val actualResult = App.read(input, dt_minus_3_days, dt)(spark)

    val expectedResult: DataFrame = Seq(
      (date7th, 1234, "john@foo.com", 4567, "activity_1", "module_1"),
      (date7th, 1234, "john@foo.com", 9876, "activity_1", "module_1"),
      (date7th, 1234, "doe@bar.com",  4567, "activity_1", "module_1"),
      (date5th, 1234, "john@foo.com", 4567, "activity_2", "module_2"),
      (date5th, 1234, "john@foo.com", 9876, "activity_2", "module_2"),
      (date5th, 1234, "doe@bar.com",  4567, "activity_2", "module_2")
    ).toDF(DATE, CLIENT_ID, USER_ID, ACCOUNT_ID, ACTIVITY, MODULE)
    assertDataFramesEquality(actualResult, expectedResult, false)
  }

  test("test countActivitiesAndModules"){
    val input: DataFrame = Seq(
      (date7th, 1234, "john@foo.com", 9876, "activity_1", "module_1"),
      (date7th, 1234, "john@foo.com", 9876, "activity_1", "module_1"),
      (date7th, 1234, "john@foo.com", 9876, "activity_2", "module_1"),
      (date7th, 1234, "john@foo.com", 9876, "activity_2", "module_2"),
      (date7th, 1234, "doe@bar.com",  4567, "activity_1", "module_1")
    ).toDF(DATE, CLIENT_ID, USER_ID, ACCOUNT_ID, ACTIVITY, MODULE)

    val actualResult = App.countActivitiesAndModules(input)

    val expectedResult: DataFrame = Seq(
      (1234, "john@foo.com", 9876, 2, 2),
      (1234, "doe@bar.com",  4567, 1, 1)
    ).toDF(CLIENT_ID, USER_ID, ACCOUNT_ID, ACTIVITIES, MODULES)

    assertDataFramesEquality(actualResult, expectedResult, false)
  }

  test("test countUsers"){
    val input: DataFrame = Seq(
      (date7th, 1234, "john@foo.com", 9876, "activity_1", "module_1"),
      (date7th, 1234, "doe@bar.com",  9876, "activity_1", "module_1"),
      (date7th, 1234, "doe@bar.com",  9876, "activity_1", "module_1"),
      (date7th, 1234, "doe@bar.com",  4567, "activity_1", "module_1")
    ).toDF(DATE, CLIENT_ID, USER_ID, ACCOUNT_ID, ACTIVITIES, MODULES)

    val actualResult = App.countUsers(input)

    val expectedResult: DataFrame = Seq(
      (1234, 9876, 2),
      (1234, 4567, 1)
    ).toDF(CLIENT_ID, ACCOUNT_ID, USERS)

    assertDataFramesEquality(actualResult, expectedResult, false)
  }


//  test("test collectActivitiesAndModules"){
//    val input: DataFrame = Seq(
//      (date7th, 1234, "john@foo.com", 9876, "activity_1", "module_1"),
//      (date7th, 1234, "john@foo.com", 9876, "activity_1", "module_1"),
//      (date7th, 1234, "john@foo.com", 9876, "activity_2", "module_1"),
//      (date7th, 1234, "john@foo.com", 9876, "activity_2", "module_2"),
//      (date7th, 1234, "doe@bar.com",  4567, "activity_1", "module_1")
//    ).toDF(DATE, CLIENT_ID, USER_ID, ACCOUNT_ID, ACTIVITY, MODULE)
//
//    val actualResult = App.collectActivitiesAndModules(input)
//
//    val expectedResult: DataFrame = Seq(
//      (date7th, 1234, "john@foo.com", 9876, Seq("activity_2", "activity_1"), Seq("module_2", "module_1")),
//      (date7th, 1234, "doe@bar.com",  4567, Seq("activity_1"), Seq("module_1"))
//    ).toDF(DATE, CLIENT_ID, USER_ID, ACCOUNT_ID, ACTIVITIES, MODULES)
//
//    assertDataFramesEquality(actualResult, expectedResult, false)
//  }
//
//  test("test collectUsers"){
//    val input: DataFrame = Seq(
//      (date7th, 1234, "john@foo.com", 9876, Seq("activity_1"), Seq("module_1")),
//      (date7th, 1234, "doe@bar.com",  9876, Seq("activity_1"), Seq("module_1")),
//      (date7th, 1234, "doe@bar.com",  9876, Seq("activity_1"), Seq("module_1")),
//      (date7th, 1234, "doe@bar.com",  4567, Seq("activity_1"), Seq("module_1"))
//    ).toDF(DATE, CLIENT_ID, USER_ID, ACCOUNT_ID, ACTIVITIES, MODULES)
//
//    val actualResult = App.collectUsers(input)
//
//    val expectedResult: DataFrame = Seq(
//      (date7th, 1234, 9876, Seq("doe@bar.com", "john@foo.com")),
//      (date7th, 1234, 4567, Seq("doe@bar.com"))
//    ).toDF(DATE, CLIENT_ID, ACCOUNT_ID, USERS)
//
//    assertDataFramesEquality(actualResult, expectedResult, false)
//  }

}
