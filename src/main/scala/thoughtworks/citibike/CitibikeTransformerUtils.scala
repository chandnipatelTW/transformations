package thoughtworks.citibike

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object CitibikeTransformerUtils {
  implicit class StringDataset(val dataSet: Dataset[Row]) {
    def computeDistances(spark: SparkSession) = {
      dataSet
        .withColumn("distance",
          HaversineDistance.UDF(
            col("start_station_latitude"),
            col("start_station_longitude"),
            col("end_station_latitude"),
            col("end_station_longitude")))
    }
  }
}
