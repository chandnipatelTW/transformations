package thoughtworks.citibike

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object HaversineDistance {
  private final val MetersPerFoot = 0.3048
  private final val FeetPerMile = 5280

  final val EarthRadiusInM: Double = 6371e3
  final val MetersPerMile: Double = MetersPerFoot * FeetPerMile

  /*
  Haversine distance ref: https://www.movable-type.co.uk/scripts/latlong.html
   */
  val UDF: UserDefinedFunction = udf((startLat: Double, startLong: Double,
                                      endLat: Double, endLong: Double) => {

    val startLatInRadians = startLat.toRadians
    val endLatInRadians = endLat.toRadians
    val latDifferenceInRadians = (endLat - startLat).toRadians;
    val longDifferenceInRadians = (endLong - startLong).toRadians;

    val a =
      Math.sin(latDifferenceInRadians / 2) * Math.sin(latDifferenceInRadians / 2) +
        Math.cos(startLatInRadians) * Math.cos(endLatInRadians) * Math.sin(longDifferenceInRadians / 2) *
          Math.sin(longDifferenceInRadians / 2)

    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))

    (EarthRadiusInM * c / MetersPerMile).formatted("%.2f").toDouble;
  })
}
