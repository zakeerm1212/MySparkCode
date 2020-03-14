package Spark_chapter_9

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object CSV_Files {

  def main(args:Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("Reading and Writing CSV File")
      .master("local").getOrCreate()

    val csvFile = spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .load("/home/cloudera/Desktop/ALL_OK/src/main/resources/Sales.csv")

    csvFile.write.format("csv")
      .option("mode","append")
      .save("/home/cloudera/Desktop/part_files_CSV")
  }
}
