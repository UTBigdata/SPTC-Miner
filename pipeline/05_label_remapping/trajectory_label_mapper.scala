// Input/output and processing details are described in the English documentation.
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.Partitioner

/** Processing module; see docs/processing_pipeline.md for the full description. */
object TrajectoryLabelMapper {
  /** Processing module; see docs/processing_pipeline.md for the full description. */
  class IdentityPartitioner(val n: Int) extends Partitioner {
    override def numPartitions: Int = n
    override def getPartition(key: Any): Int = key.asInstanceOf[Int]
  }
  def run(spark: SparkSession): Unit = {
    // Spark Scala / spark-shell script
    // Stage 2: run AFTER cluster.py creates groupID/clusterID for HIGH-FREQUENCY feature ODs.
    // Input/output and processing details are described in the English documentation.

    val outRoot = "hdfs://namenode.example.org:9000/project/data/201901And02/Withpositions"
    // Read the EXACT output directory used in the cluster.py spark-submit command below.
    val clusterPath = outRoot + "/clusetered_center_point"
    val m = 64                // Number of physical RDD partitions; keep consistent with experiment.
    val minsup = 200L         // Example only: set to the actual experiment's minimum support.

    // U: every OD/frequency/original position. F: only high-frequency OD cluster features.
    val summaries = spark.read.parquet(outRoot + "/Complete_OD_Summary_U")
    val features = spark.read.parquet(outRoot + "/Feature_Vector_F")
    val clusterRaw = spark.read.option("header", "true").csv(clusterPath)
    val clusterLabels = clusterRaw.select(
        concat(col("station1"), lit(" -> "), col("station2")).as("od"),
        col("groupID").cast(IntegerType).as("groupID"),
        col("clusterID").cast(IntegerType).as("clusterID")
      ).dropDuplicates(Seq("od", "groupID", "clusterID"))

    // Each feature OD should have a deterministic cluster label.
    val duplicateLabels = clusterLabels.groupBy("od").count().filter(col("count") > 1).count()
    require(duplicateLabels == 0, "One high-frequency OD has multiple cluster labels")

    val featureODs = features.select(col("CardID"), explode(col("F")).as("item")).select(col("CardID"), col("item.od").as("od"))
    val mappedLabels = featureODs.join(clusterLabels, Seq("od"), "left")
    val unmatched = mappedLabels.filter(col("groupID").isNull).count()
    require(unmatched == 0, s"Cluster labels missing for $unmatched feature-OD records")

    // Partition selection uses ONLY F labels, but the WHOLE U is copied unchanged.
    val mapping = mappedLabels.select("CardID", "groupID", "clusterID").distinct().withColumn("partitionID", pmod(col("groupID"), lit(m)))
    val dfMultiPartition = mapping.join(summaries, Seq("CardID"), "inner").join(features, Seq("CardID"), "inner").select("CardID", "U", "F", "groupID", "clusterID", "partitionID")

    // If multiple feature labels map to the same physical partition, retain one FULL U copy per card.
    val dfFinal = dfMultiPartition.groupBy("CardID", "partitionID").agg(first(col("U"), ignoreNulls = true).as("U"),
           first(col("F"), ignoreNulls = true).as("F"),
           min(col("groupID")).as("groupID"),
           min(col("clusterID")).as("clusterID")).select("CardID", "U", "F", "groupID", "clusterID", "partitionID")

    // Explicit RDD partitioner as in the original experiment; use partitionID as the key.

    val keyed = dfFinal.rdd.map(row => (row.getAs[Int]("partitionID"), row))
    val partitioned = keyed.partitionBy(new IdentityPartitioner(m))
    val dfRepartitioned = spark.createDataFrame(partitioned.values, dfFinal.schema)
    // partitionBy on write explicitly preserves the logical partitionID for downstream consumers.
    dfRepartitioned.write.mode("overwrite").partitionBy("partitionID").parquet(outRoot + "/repartition_complete_U")

    // Feed the existing TPGI code by loading FULL U + positions; not Feature_Vector alone.
    val indexedInput = spark.read.parquet(outRoot + "/repartition_complete_U")
    val postings = indexedInput.select(col("partitionID"), col("CardID"), explode(col("U")).as("item")).select(col("partitionID"), col("CardID"),
        col("item.od").as("od"), col("item.freq").as("Frequency"),
        col("item.Pos").as("Positions"))
    val localODCount = postings.groupBy("partitionID", "od").agg(sum(col("Frequency")).as("ODCount"))
    val localFrequent = localODCount.filter(col("ODCount") >= lit(minsup))
    val localPostings = postings.join(localFrequent.select("partitionID", "od"),
      Seq("partitionID", "od"), "inner")
    localODCount.write.mode("overwrite").parquet(outRoot + "/Local_OD_Counts")
    localFrequent.write.mode("overwrite").parquet(outRoot + "/Local_Frequent_ODs")
    localPostings.write.mode("overwrite").parquet(outRoot + "/Local_TPGI_Postings")

    // Shared global topology directory: global thresholding BEFORE partition replication.
    val globalODCounts = spark.read.parquet(outRoot + "/Global_OD_Counts")
    val globalEdges = globalODCounts.filter(col("GlobalFrequency") >= lit(minsup)).select(split(col("Transfer_Route"), " -> ").getItem(0).as("A"),
              split(col("Transfer_Route"), " -> ").getItem(1).as("B"))
    val globalAdj = globalEdges.groupBy("A").agg(collect_set(col("B")).as("Neighbors"))
    globalAdj.write.mode("overwrite").parquet(outRoot + "/Global_Topology_Adjacency")

    // Coverage/consistency checks.
    val allCards = summaries.select("CardID").distinct()
    val missingCards = allCards.join(dfFinal.select("CardID").distinct(), Seq("CardID"), "left_anti").count()
    val badPostings = postings.filter(size(col("Positions")) =!= col("Frequency")).count()
    val duplicatedCardsWithinPartition = dfFinal.groupBy("CardID", "partitionID").count().filter(col("count") > 1).count()
    require(missingCards == 0, "Some original trajectories are missing after F-based mapping")
    require(badPostings == 0, "Positions have been lost or damaged during mapping")
    require(duplicatedCardsWithinPartition == 0, "A card was duplicated in the same physical partition")
    println(s"OK: missing cards=$missingCards, bad postings=$badPostings, duplicate copies=$duplicatedCardsWithinPartition")

    // Important theoretical distinction: every trajectory surviving mapping does not automatically
    // imply that every globally frequent OD has >=minsup occurrences in some ONE local partition.
    // Check this property separately before claiming partition-independent mining completeness.
    val globalFreq = globalODCounts.filter(col("GlobalFrequency") >= lit(minsup)).select(col("Transfer_Route").as("od"), col("GlobalFrequency"))
    val maxLocal = localODCount.groupBy("od").agg(max(col("ODCount")).as("MaxLocalFrequency"))
    val splitFrequentODs = globalFreq.join(maxLocal, Seq("od"), "left").filter(coalesce(col("MaxLocalFrequency"), lit(0L)) < lit(minsup))
    splitFrequentODs.write.mode("overwrite").parquet(outRoot + "/QA_GlobalFreq_LocalInfrequent")
    println(s"WARNING check: globally frequent ODs missed by ALL local 1-OD filters = ${splitFrequentODs.count()}")
  }
}

TrajectoryLabelMapper.run(spark)
