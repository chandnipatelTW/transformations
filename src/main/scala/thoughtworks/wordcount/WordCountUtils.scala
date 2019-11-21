package thoughtworks.wordcount

import org.apache.spark.sql.{Dataset, SparkSession}

object WordCountUtils {
  implicit class StringDataset(val dataSet: Dataset[String]) {
    def splitWords(spark: SparkSession) = {
      import spark.implicits._
      dataSet
        .flatMap(x => x.split("[\\s\\.;\\-,\"]+"))
        .filter(x => x != "")
    }

    def countByWord(spark: SparkSession) = {
      import spark.implicits._
      dataSet
          .map(word => word.toLowerCase)
          .groupBy($"value".as("word"))
          .count()
          .orderBy($"word")
    }
  }
}
