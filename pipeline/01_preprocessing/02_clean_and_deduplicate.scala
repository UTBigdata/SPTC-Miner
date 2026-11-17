// Input/output and processing details are described in the English documentation.
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** Processing module; see docs/processing_pipeline.md for the full description. */
object RawInputCleaning {
  def run(spark: SparkSession): Unit = {
    import spark.implicits._
    val ticketdata =spark.read.option("inferSchema","false").option("seq",",").option("encoding","UTF-8").option("header","true").csv("hdfs://namenode.example.org:9000/project/DATA/201901/TicketData")
    val busgps =spark.read.option("inferSchema","false").option("seq",",").option("encoding","UTF-8").option("header","true").csv("hdfs://namenode.example.org:9000/project/DATA/201901/BusGPS")
    val subwaygps =spark.read.option("inferSchema","false").option("seq",",").option("encoding","UTF-8").option("header","true").csv("hdfs://namenode.example.org:9000/project/data/GPSdata")
    val busdata =spark.read.option("inferSchema","false").option("seq",",").option("encoding","UTF-8").option("header","true").csv("hdfs://namenode.example.org:9000/project/data/stations/*/*.csv")
    val df01 = ticketdata.na.drop() 
    val DF01 = busgps.na.drop() 
    val Df01 = subwaygps.na.drop() 
    val dF01 = busdata.na.drop()
    val DF03 = DF01.dropDuplicates(Seq("idx","opath","linenumber","time"))   
    val Df03 = Df01.dropDuplicates(Seq("station")) 
    df01.write.format("csv").option("header", "true").mode("overwrite").save("hdfs://namenode.example.org:9000/project/DATA/201901/Ticket_Data")
    //DF03.write.format("csv").option("header", "true").mode("overwrite").save("hdfs://namenode.example.org:9000/project/DATA/201901/Bus_Data")
    Df03.write.format("csv").option("header", "true").mode("overwrite").save("hdfs://namenode.example.org:9000/project/DATA/201901/Subway_Data")
  }
}

RawInputCleaning.run(spark)
