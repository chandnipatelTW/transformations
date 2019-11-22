package thoughtworks.citibike

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, StructField}
import thoughtworks.DefaultFeatureSpecWithSpark

class CitibikeTransformerUtilsTest extends DefaultFeatureSpecWithSpark {

  import spark.implicits._
  val citibikeBaseDataColumns = Seq(
    "tripduration", "starttime", "stoptime", "start_station_id", "start_station_name", "start_station_latitude", "start_station_longitude", "end_station_id", "end_station_name", "end_station_latitude", "end_station_longitude", "bikeid", "usertype", "birth_year", "gender"
  )

  feature("Citibike Transformer Utils") {
    scenario("Should calculate distance for Given Dataset") {

      Given("Dataset with start and end latitudes and longitude")
      val sampleCitibikeData = Seq(
        (328, "2017-07-01 00:00:08", "2017-07-01 00:05:37", 3242, "Schermerhorn St & Court St", 40.69102925677968, -73.99183362722397, 3397, "Court St & Nelson St", 40.6763947, -73.99869893, 27937, "Subscriber", 1984, 2)
      )
      val inputDF = sampleCitibikeData.toDF(citibikeBaseDataColumns: _*)

      When("computeDistances is called")
      val outputDF = CitibikeTransformerUtils.StringDataset(inputDF).computeDistances(spark)

      Then("The new data should have distance column with value")
      val expectedData = Array(
        Row(328, "2017-07-01 00:00:08", "2017-07-01 00:05:37", 3242, "Schermerhorn St & Court St", 40.69102925677968, -73.99183362722397, 3397, "Court St & Nelson St", 40.6763947, -73.99869893, 27937, "Subscriber", 1984, 2, 1.07)
      )
      outputDF.schema("distance") should be(StructField("distance", DoubleType, nullable = true))
      outputDF.collect should be(expectedData)
    }

    scenario("Should calculate distance for Given Dataset with null values") {
      Given("Dataset with start and end latitudes and longitude as null")
      val sampleCitibikeData = Seq(
        (328, "2017-07-01 00:00:08", "2017-07-01 00:05:37", 3242, "Schermerhorn St & Court St", null, -73.99183362722397, 3397, "Court St & Nelson St", 40.6763947, -73.99869893, 27937, "Subscriber", 1984, 2)
      )
      val inputDF = sampleCitibikeData.toDF(citibikeBaseDataColumns: _*)

      When("computeDistances is called")
      val outputDF = CitibikeTransformerUtils.StringDataset(inputDF).computeDistances(spark)

      Then("The new data should have distance column with null value")
      val expectedData = Array(
        Row(328, "2017-07-01 00:00:08", "2017-07-01 00:05:37", 3242, "Schermerhorn St & Court St", null, -73.99183362722397, 3397, "Court St & Nelson St", 40.6763947, -73.99869893, 27937, "Subscriber", 1984, 2, null)
      )
      outputDF.schema("distance") should be(StructField("distance", DoubleType, nullable = true))
      outputDF.collect should be(expectedData)
    }
  }

}