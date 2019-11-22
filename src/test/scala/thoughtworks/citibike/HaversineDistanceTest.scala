package thoughtworks.citibike

import org.apache.spark.sql.Row
import thoughtworks.DefaultFeatureSpecWithSpark
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, StructField}

class HaversineDistanceTest extends DefaultFeatureSpecWithSpark {

  feature("Haversine Distance"){
    scenario("UDF should calculate distance"){
      Given("Start and end latitudes and longitudes")
      import spark.implicits._
      val inputDF = Seq((40.69102925677968,-73.99183362722397,40.6763947,-73.99869893)).toDF

      When("UDF is applied")
      val outputDF = inputDF.withColumn("distance", HaversineDistance.UDF(col("_1"), col("_2"),
        col("_3"), col("_4")))

      Then("Distance should be returned")
      val expectedData = Array(
        Row(40.69102925677968, -73.99183362722397, 40.6763947, -73.99869893, 1.07)
      )
      outputDF.schema("distance") should be(StructField("distance", DoubleType, nullable = true))
      outputDF.collect should be(expectedData)
    }
  }
}
