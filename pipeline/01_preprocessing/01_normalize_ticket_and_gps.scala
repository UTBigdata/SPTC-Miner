// Input/output and processing details are described in the English documentation.
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.DoubleType

/** Processing module; see docs/processing_pipeline.md for the full description. */
object RawPlateNormalization {
  def run(spark: SparkSession): Unit = {
    import spark.implicits._
    {
    // Input/output and processing details are described in the English documentation.
    val inputdata =spark.read.option("inferSchema","false").option("seq",",").option("encoding","UTF-8").option("header","true").csv("hdfs://namenode.example.org:9000/project/DATA/201901/inputdata/201901")
      val df01 = inputdata.filter( col("车牌号").startsWith("粤BU"))   
      val df02 = inputdata.withColumn("车牌号",regexp_replace(col("车牌号"),"^(粤BU[^D]*)$","$1D"))
      val df03 = df02.filter(col("车牌号").rlike("^[0-9].*D$"))     
      val df04 = df02.withColumn("车牌号",regexp_replace(col("车牌号"),"^(\\d.*D)$","粤B$1"))
      val df05 = df04.filter( col("车牌号").startsWith("粤BBE"))    
      val df06 = df04.withColumn("车牌号",regexp_replace(col("车牌号"),"^(粤BBE[^D]*)$","$1D"))
      val df07 = df06.filter(col("车牌号").rlike("^[0-9]"))      
      val df08 = df06.withColumn("车牌号",concat(lit("粤B"), col("车牌号"), lit("D"))) 
      val df09 = df08.withColumn("车牌号", regexp_replace(col("车牌号"), "^粤B粤B(.*)DD$", "粤B$1D"))
      df09.write.format("csv").option("header", "true").mode("overwrite").save("hdfs://namenode.example.org:9000/project/DATA/201901/TicketData")

    }

    {
    // Input/output and processing details are described in the English documentation.
    val gpsdata = {
      val gpsdata =spark.read.option("inferSchema","false").option("seq",",").option("encoding","UTF-8").option("header","true").csv("hdfs://namenode.example.org:9000/reference-data/SZT_200/bus_data/201901/fmm_matched_data")
      val busdata =spark.read.option("inferSchema","false").option("seq",",").option("encoding","UTF-8").option("header","true").csv("hdfs://namenode.example.org:9000/project/data/stations/*/*.csv")
      val GPSdata = gpsdata.withColumn("split_array", split($"idx;opath;linenumber;lng;lat;time;pgeom", ";")).select($"idx;opath;linenumber;lng;lat;time;pgeom",$"split_array"(0).as("idx"),$"split_array"(1).as("opath"),$"split_array"(2).as("linenumber"),$"split_array"(3).as("longitude"),$"split_array"(4).as("latitude"),$"split_array"(5).as("time"))
      GPSdata.select("idx","opath","linenumber","longitude","latitude","time")
    }
    val df01 = gpsdata.filter( col("idx").startsWith("SZBU")) 
    val df02 = gpsdata.withColumn("idx",when(col("idx").startsWith("SZBU"),concat(lit("粤BU"), regexp_replace(col("idx"), "^SZBU", ""), lit("D"))).otherwise(col("idx")))
    val df03 = df02.filter(col("idx").rlike("BS.*D$"))     
    val df04= df02.withColumn("idx",when(col("idx").rlike("^BS.*D$"), regexp_replace(col("idx"), "^BS", "粤B")))
    val df05 = df04.filter( col("idx").startsWith("BS"))    
    val df06 = df04.withColumn("idx",when(col("idx").startsWith("BS"),concat(lit("粤B"), regexp_replace(col("idx"), "^BS", ""), lit("D"))).otherwise(col("idx")))
    val df07 = df06.filter( col("idx").startsWith("SZBBE"))   
    val df08 = df06.withColumn("idx",when(col("idx").startsWith("SZBBE"),concat(lit("粤BBE"), regexp_replace(col("idx"), "^SZBBE", ""), lit("D"))).otherwise(col("idx")))
    val df09 = df08.withColumn("idx", regexp_replace(col("idx"), "^粤B粤B(.*)DD$", "粤B$1D"))
    df09.write.format("csv").option("header", "true").mode("overwrite").save("hdfs://namenode.example.org:9000/project/DATA/201901/BusGPS")
    }
  }
}

RawPlateNormalization.run(spark)
