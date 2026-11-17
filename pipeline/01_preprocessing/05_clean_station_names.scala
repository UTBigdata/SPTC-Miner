// Input/output and processing details are described in the English documentation.
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** Processing module; see docs/processing_pipeline.md for the full description. */
object StationEventCleaner {
  def run(spark: SparkSession): Unit = {
    import spark.implicits._
    val inputdata =spark.read.option("inferSchema","false").option("seq",",").option("encoding","UTF-8").option("header","true").csv("hdfs://namenode.example.org:9000/project/DATA/201901/data")
    val cleanedDF = inputdata.withColumn("station_cleaned", regexp_replace(col("station"), "[①②③④⑤⑥⑦⑧⑨⑩\\d]+", ""))
    val df01 = cleanedDF.select("CardID","Time","Type","station_cleaned","line","carID","longitude","latitude")
    val newColumnNames = Seq("CardID", "Time", "Type","station","line","carID","longitude","latitude")
    val df02 = df01.toDF(newColumnNames: _*)
    val df03 = df02.na.drop()
    val df04 = df03.dropDuplicates(Seq("CardID","Time"))
    df04.write.format("csv").option("header", "true").mode("overwrite").save("hdfs://namenode.example.org:9000/project/DATA/201901/newdata")
  }
}

StationEventCleaner.run(spark)
