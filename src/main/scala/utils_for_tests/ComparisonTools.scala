package utils_for_tests

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions.col


/**
  * Methods to compare DataFrames for unit tests.
  */
object ComparisonTools {

  val NUM_ROWS = 25

  val ERROR_MSG_SCHEMA: String = "\n\nOutputs should have matching schemas: \n"
  val ERROR_MSG_COLUMNS: String = "\n\nOutputs must have the same column names: \n"
  val ERROR_MSG_SIZE: String = "\n\nOutputs must have an equal number of rows"
  val ERROR_MSG_CONTENT: String = "Outputs are not equal"

  def assertDataFramesEquality(actual: DataFrame, expected: DataFrame, compareSchema: Boolean, outputToConsole: Boolean): Unit = {
    actual.cache()
    expected.cache()
    
    if (outputToConsole) {
      printDSToConsole(actual, expected)
    }
    
    if (compareSchema) {
      compareSchemas(actual, expected)
    }

    val columns = compareColumns(actual, expected)
    val expected_rdd = expected.select(columns:_*).rdd
    val actual_rdd = actual.select(columns:_*).rdd

    compareSize(actual_rdd, expected_rdd)
    compareContent(actual_rdd, expected_rdd)
  }

  
  def compareSchemas(actual: Dataset[_], expected: Dataset[_]): Unit = {
    assert(actual.schema == expected.schema,
      s"$ERROR_MSG_SCHEMA" +
        s"Actual Output:\n${actual.schema.treeString}" +
        s"Expected Output:\n${expected.schema.treeString}"
    )
  }

  def compareColumns(actual: Dataset[_], expected: Dataset[_]): Array[Column] = {
    val actualColumns = actual.columns.sorted
    val expectedColumns = expected.columns.sorted
    assert(actualColumns sameElements expectedColumns,
      s"$ERROR_MSG_COLUMNS" +
        s"Actual Output: \n[${actualColumns.mkString(", ")}]\n" +
        s"Expected Output:  \n[${expectedColumns.mkString(", ")}]")
    actualColumns.map(col)
  }
  
  def compareSize(actual: RDD[_], expected: RDD[_]): Unit = {
    assert(actual.count() == expected.count(),
      s"$ERROR_MSG_SIZE")
  }
  
  def compareContent(actual: RDD[_], expected: RDD[_]): Unit = {
    val actualRows = actual.collect().toList
    val expectedRows = new ListBuffer[Any]() ++ expected.collect()
    val actualRowsWithNoMatch = new ListBuffer[Any]()
    
    actualRows.foreach(row => {
      val index = expectedRows.indexOf(row)
      if (index != -1) {
        expectedRows.remove(index)
      } else {
        actualRowsWithNoMatch.+=(row)
      }
    })
    
    val failureOccurred = expectedRows.nonEmpty || actualRowsWithNoMatch.nonEmpty
    if (failureOccurred) {
      printBadRows(actualRowsWithNoMatch, "Actual rows with no match in the expected output")
      printBadRows(expectedRows, "Expected rows with no match in the actual output")
    }
    assert(!failureOccurred, ERROR_MSG_CONTENT)
  }
  
  private def printBadRows(rows: ListBuffer[_], errorMsg: String): Unit = {
    if (rows.nonEmpty) {
      println(s"$errorMsg: \n${rows.mkString("\n")}")
    }
  }

  private def printDSToConsole(actual: Dataset[_], expected: Dataset[_]): Unit = {
    println("Actual schema:")
    actual.printSchema()
    println("Expected schema:")
    expected.printSchema()
    
    println("Actual data:")
    actual.show(NUM_ROWS,false)
    println("Expected data:")
    expected.show(NUM_ROWS,false)
  }
  
}
