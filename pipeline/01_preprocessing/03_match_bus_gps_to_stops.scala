// Input/output and processing details are described in the English documentation.
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import ch.hsr.geohash.GeoHash
import org.apache.spark.sql.expressions.Window

/** Processing module; see docs/processing_pipeline.md for the full description. */
object BusStopGeoMatcher {
  def run(spark: SparkSession): Unit = {
    import spark.implicits._
    val busgps =spark.read.option("inferSchema","false").option("seq",",").option("encoding","UTF-8").option("header","true").csv("hdfs://namenode.example.org:9000/project/DATA/201901/Bus_Data")
    val busdata =spark.read.option("inferSchema","false").option("seq",",").option("encoding","UTF-8").option("header","true").csv("hdfs://namenode.example.org:9000/project/data/stations/*/*.csv")
    val df01 = {
      val windowSpec = Window.partitionBy("idx", "linenumber").orderBy("opath", "time")
      busgps.withColumn("rank_in_group", row_number().over(windowSpec)).orderBy("idx", "opath","linenumber", "time").drop("rank_in_group")
    }
    val df02 = busdata.withColumn("position", col("position").cast("int"))
    val df03 = {
      val windowSpec = Window.partitionBy("direction", "existLine_id").orderBy("position")
      df02.withColumn("rank_in_group", row_number().over(windowSpec)).orderBy("existLine_id", "direction", "position").drop("rank_in_group")
    }
    import org.apache.spark.sql.types.DoubleType
    import ch.hsr.geohash.GeoHash
    def geoHash(lat: Double, lon: Double, precision: Int): String = {
      GeoHash.geoHashStringWithCharacterPrecision(lat, lon, precision)
    }
    val geoHashUdf = udf(geoHash _)
    val df04 = df01.withColumn("lon", col("longitude").cast("double")).withColumn("lat", col("latitude").cast("double")).withColumn("geohash", geoHashUdf($"lat", $"lon",lit(7))).repartition($"linenumber", $"geohash")
    val df05 = df03.withColumn("lon", col("y").cast("double")).withColumn("lat", col("x").cast("double")).withColumn("geohash", geoHashUdf($"lat", $"lon", lit(7)))
    val epsilon = 0.00015
    val joined = df04.alias("a").join(broadcast(df05.alias("b")),$"a.linenumber" === $"b.existLine_id" &&$"a.geohash" === $"b.geohash" &&abs($"a.lon" - $"b.lon") < epsilon &&abs($"a.lat" - $"b.lat") < epsilon)
    val df06 = joined.select("idx","linenumber","name","longitude","latitude","time","direction","position")
    val df07 = df06.dropDuplicates(Seq("idx","name","linenumber","time"))
    val df08 = df07.na.drop()
    df08.write.format("csv").option("header", "true").mode("overwrite").save("hdfs://namenode.example.org:9000/project/DATA/201901/busWithstation")
  }
}

BusStopGeoMatcher.run(spark)
