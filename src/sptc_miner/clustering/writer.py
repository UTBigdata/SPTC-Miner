'Write labeled OD clusters as a Spark CSV dataset.'

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

def save_output_csv_df(output_path, cluster_list):
    'Save spatial cluster labels with eleven stable column names.'
    spark = SparkSession.builder.getOrCreate()

  
    schema = StructType([
        StructField("station1", StringType(), True),
        StructField("station2", StringType(), True),
        StructField("weight", DoubleType(), True),
        StructField("lon1", DoubleType(), True),
        StructField("lat1", DoubleType(), True),
        StructField("lon2", DoubleType(), True),
        StructField("lat2", DoubleType(), True),
        StructField("center_lon", DoubleType(), True),
        StructField("center_lat", DoubleType(), True),
        StructField("groupID", IntegerType(), True),
        StructField("clusterID", IntegerType(), True),
    ])

    df = spark.createDataFrame(cluster_list, schema)
  
    df.coalesce(1).write.csv(output_path, header=True, mode="overwrite")
