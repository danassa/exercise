package exercise

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object Context {

  def initSpark(appName: String, isLocal: Boolean): SparkSession = {
    val sparkConf = buildSparkConf(appName, isLocal)
    val sparkSession = buildSparkSession(sparkConf)
    sparkSession
  }

  private def buildSparkConf(appName: String, isLocal: Boolean): SparkConf = {
    val conf = new SparkConf().setAppName(appName)
    if (isLocal)
      conf.setMaster("local[*]")
    else
      conf.setMaster("yarn-cluster")
  }

  private def buildSparkSession(sparkConf: SparkConf): SparkSession = {
    SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()
  }

}
