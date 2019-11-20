package thoughtworks.wordcount

import org.apache.spark.sql.{Dataset, SparkSession}

case class WordCountRow(word: String, count: BigInt)

object WordCountUtils {
  implicit class StringDataset(val dataSet: Dataset[String]) {
    def splitWords(spark: SparkSession) = {
      import spark.implicits._
      dataSet.flatMap(x => x.split("\\s+"))
        .flatMap(x => x.split("\\.+"))
        .flatMap(x => x.split(",+"))
        .flatMap(x => x.split("-+"))
        .flatMap(x => x.split(";+"))
        .filter(x => x != "")
    }

    def countByWord(spark: SparkSession) = {
      import spark.implicits._
      dataSet
          .map(word => word.toLowerCase)
          .groupBy($"value".as("word")).count().as[WordCountRow]
    }
  }
}
