// Input/output and processing details are described in the English documentation.
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.Path
import org.apache.spark.storage.StorageLevel

/** Processing module; see docs/processing_pipeline.md for the full description. */
object TrajectorySummaryBuilder {
  def run(spark: SparkSession): Unit = {
    import spark.implicits._

    spark.conf.set("spark.sql.session.timeZone", "Asia/Shanghai")

    // ---------- 0. Paths ----------
    val outRoot = "hdfs://namenode.example.org:9000/project/data/201901/Withpositions"
    val featureThreshold = 0.80

    val upstreamCandidates = Seq(
      "hdfs://namenode.example.org:9000/project/DATA/201901/$base/bus_alighting_transfer_MP2012",
      "hdfs://namenode.example.org:9000/project/DATA/201901/bus_alighting_transfer_MP2012",
      "hdfs://namenode.example.org:9000/project/experiment_data/bus_alighting_transfer_MP2012"
    )

    def existsHdfs(path: String): Boolean = {
      val p = new Path(path)
      p.getFileSystem(spark.sparkContext.hadoopConfiguration).exists(p)
    }

    val upstreamRoot = upstreamCandidates.find(p =>
      existsHdfs(p + "/transfer_events") &&
      existsHdfs(p + "/inferred_bus_alighting")
    ).getOrElse(throw new IllegalArgumentException(
      "Cannot find upstream results. Paths tested: " + upstreamCandidates.mkString(" | ")))

    // raw newdata is READ ONLY. The first candidate is colocated with upstream.
    val rawTapCandidates = Seq(
      upstreamRoot.stripSuffix("/bus_alighting_transfer_MP2012") + "/newdata",
      "hdfs://namenode.example.org:9000/project/DATA/201901/newdata",
      "hdfs://namenode.example.org:9000/project/data/201901/newdata",
      "hdfs://namenode.example.org:9000/project/experiment_data/newdata"
    )
    val rawTapPath = rawTapCandidates.find(existsHdfs).getOrElse(
      throw new IllegalArgumentException(
        "Missing newdata for coordinates. Paths tested: " + rawTapCandidates.mkString(" | ")))

    println("UPSTREAM transfer inputs : " + upstreamRoot)
    println("READ-ONLY newdata        : " + rawTapPath)
    println("LEGACY output            : " + outRoot)

    // ---------- 1. Use the ACTUAL upstream schema, preserve unique event positions ----------
    val transferInput = spark.read.parquet(upstreamRoot + "/transfer_events").select(
        trim(col("CardID").cast("string")).as("CardID"),
        col("position").cast(IntegerType).as("source_position"),
        col("event_time").cast(TimestampType).as("EventTime"),
        col("next_board_time").cast(TimestampType).as("NextEventTime"),
        col("from_system"), col("from_station"),
        col("to_system"), col("to_station"),
        col("distance_m").cast(DoubleType).as("verified_distance_m")
      ).persist(StorageLevel.DISK_ONLY)

    val namedTransfer = transferInput.withColumn("Transfer_Route",
        when(col("from_system") === "BUS" && col("to_system") === "METRO",
          concat(lit("Bus_"), col("from_station"), lit(" -> Subway_"), col("to_station")))
        .when(col("from_system") === "METRO" && col("to_system") === "BUS",
          concat(lit("Subway_"), col("from_station"), lit(" -> Bus_"), col("to_station")))
      ).persist(StorageLevel.DISK_ONLY)

    val nInput = namedTransfer.count()
    require(nInput > 0L, "transfer_events is empty")

    val malformed = namedTransfer.filter(
      col("CardID").isNull || col("source_position").isNull ||
      col("source_position") <= 0 || col("EventTime").isNull ||
      col("NextEventTime").isNull || col("from_station").isNull ||
      col("to_station").isNull || col("Transfer_Route").isNull ||
      col("NextEventTime").cast("long") < col("EventTime").cast("long")
    ).count()
    require(malformed == 0L, "Invalid input transfer rows: " + malformed)

    // Upstream 'position' is already 1-based chronological row_number per CardID.
    // Check uniqueness and completeness before using it as the stable event key.
    val badPositions = namedTransfer.groupBy("CardID").agg(
        count(lit(1)).as("n"),
        countDistinct(col("source_position")).as("distinct_pos"),
        min(col("source_position")).as("min_pos"),
        max(col("source_position")).as("max_pos")).filter(col("n") =!= col("distinct_pos") ||
              col("min_pos") =!= lit(1) || col("max_pos") =!= col("n")).count()
    require(badPositions == 0L,
      "Upstream transfer position is nonunique or not 1..N for " + badPositions + " cards")

    // ---------- 2. Prepare READ-ONLY newdata geography lookups ----------
    // These are the same raw IC tap fields that produced the upstream MP2012 outputs.
    val rawTaps = spark.read.option("header", "true").option("encoding", "UTF-8").option("inferSchema", "false").csv(rawTapPath).select(
        trim(col("CardID")).as("CardID"),
        trim(col("Time")).as("raw_time"),
        lower(trim(col("Type"))).as("type_lc"),
        trim(col("station")).as("station"),
        trim(col("line")).as("line"),
        regexp_replace(trim(col("carID")), "\\s+", "").as("carID"),
        col("longitude").cast(DoubleType).as("lon"),
        col("latitude").cast(DoubleType).as("lat")
      ).withColumn("tap_ts", coalesce(
        to_timestamp(col("raw_time"), "yyyyMMddHHmmss"),
        to_timestamp(col("raw_time"), "yyyy-MM-dd HH:mm:ss"))).withColumn("kind",
        when(col("type_lc").rlike("巴士|公交|bus"), lit("BUS_IN")).when(col("type_lc").rlike("地铁|metro") &&
              col("type_lc").rlike("进站|入站|entry"), lit("METRO_IN")).when(col("type_lc").rlike("地铁|metro") &&
              col("type_lc").rlike("出站|出闸|exit"), lit("METRO_OUT"))).filter(col("CardID").isNotNull && col("tap_ts").isNotNull &&
              col("station").isNotNull && col("kind").isNotNull &&
              col("lon").isNotNull && col("lat").isNotNull).select("CardID", "tap_ts", "station", "line", "carID", "kind", "lon", "lat")

    // Restrict the large raw CSV to tap events required by accepted transfer results.
    val requiredTapKeys = namedTransfer.filter(col("from_system") === "METRO").select(col("CardID"), col("EventTime").as("tap_ts"), lit("METRO_OUT").as("kind")).unionByName(namedTransfer.filter(col("to_system") === "METRO").select(col("CardID"), col("NextEventTime").as("tap_ts"),
                lit("METRO_IN").as("kind"))).unionByName(namedTransfer.filter(col("to_system") === "BUS").select(col("CardID"), col("NextEventTime").as("tap_ts"),
                lit("BUS_IN").as("kind"))).distinct()

    val usedTaps = rawTaps.join(requiredTapKeys,
      Seq("CardID", "tap_ts", "kind"), "left_semi")

    val metroOrder = Window.partitionBy("CardID", "tap_ts", "kind", "station").orderBy(col("lon").asc, col("lat").asc)
    val metroGeo = usedTaps.filter(col("kind").isin("METRO_IN", "METRO_OUT")).withColumn("_rn", row_number().over(metroOrder)).filter(col("_rn") === 1).drop("_rn", "line", "carID").persist(StorageLevel.DISK_ONLY)

    val busRawUsed = usedTaps.filter(col("kind") === "BUS_IN")
    val ambiguousBusTap = busRawUsed.groupBy("CardID", "tap_ts").agg(countDistinct(struct("station", "line", "carID", "lon", "lat")).as("n")).filter(col("n") > 1L).count()
    require(ambiguousBusTap == 0L,
      "Ambiguous same-second BUS_IN coordinates in newdata: " + ambiguousBusTap +
      ". Cannot safely recover exact BUS boarding coordinates without upstream point IDs.")
    val busOrder = Window.partitionBy("CardID", "tap_ts").orderBy(col("station").asc, col("line").asc, col("carID").asc)
    val busGeo = busRawUsed.withColumn("_rn", row_number().over(busOrder)).filter(col("_rn") === 1).drop("_rn", "kind", "line", "carID").persist(StorageLevel.DISK_ONLY)

    // ---------- 3. BUS END coordinates from the actual inferred alighting ----------
    // Unlike legacy point-ID join, match CardID + actual alight timestamp + stop,
    // AND the next observed tap timestamp/station. Only NEXT_TAP is a true transfer.
    val inferred = spark.read.parquet(upstreamRoot + "/inferred_bus_alighting").filter(col("anchor_type") === "NEXT_TAP" && col("next_kind") === "METRO_IN").select(
        trim(col("CardID").cast("string")).as("CardID"),
        col("alight_ts").cast(TimestampType).as("EventTime"),
        col("next_ts").cast(TimestampType).as("NextEventTime"),
        col("alight_station").as("from_station"),
        col("next_station").as("to_station"),
        col("event_id"),
        col("alight_lon").cast(DoubleType).as("lon"),
        col("alight_lat").cast(DoubleType).as("lat")
      )

    val inferredKeys = Seq("CardID", "EventTime", "NextEventTime",
      "from_station", "to_station")
    val relevantInferredKeys = namedTransfer.filter(col("from_system") === "BUS").select(inferredKeys.map(col): _*).distinct()
    val usedInferred = inferred.join(relevantInferredKeys, inferredKeys, "left_semi")
    val ambiguousAlighting = usedInferred.groupBy(inferredKeys.map(col): _*).agg(countDistinct(col("event_id")).as("n")).filter(col("n") > 1L).count()
    require(ambiguousAlighting == 0L,
      "More than one inferred BUS END fits the same transfer: " + ambiguousAlighting)

    val inferOrder = Window.partitionBy(inferredKeys.map(col): _*).orderBy(col("event_id").asc)
    val inferredGeo = usedInferred.withColumn("_rn", row_number().over(inferOrder)).filter(col("_rn") === 1).drop("_rn", "event_id")

    // ---------- 4. Build 1:1 coordinate maps using the upstream source_position ----------
    val busFromGeo = namedTransfer.filter(col("from_system") === "BUS").join(inferredGeo, inferredKeys, "left").select(col("CardID"), col("source_position"),
              col("lon").as("Longitude"), col("lat").as("Latitude"))

    val metroFromGeo = namedTransfer.filter(col("from_system") === "METRO").join(metroGeo.select(
        col("CardID"), col("tap_ts").as("EventTime"),
        col("station").as("from_station"), col("kind"), col("lon"), col("lat")),
        Seq("CardID", "EventTime", "from_station"), "left").filter(col("kind") === "METRO_OUT" || col("kind").isNull).select(col("CardID"), col("source_position"),
              col("lon").as("Longitude"), col("lat").as("Latitude"))

    val fromGeo = busFromGeo.unionByName(metroFromGeo)

    val metroToGeo = namedTransfer.filter(col("to_system") === "METRO").join(metroGeo.select(
        col("CardID"), col("tap_ts").as("NextEventTime"),
        col("station").as("to_station"), col("kind"), col("lon"), col("lat")),
        Seq("CardID", "NextEventTime", "to_station"), "left").filter(col("kind") === "METRO_IN" || col("kind").isNull).select(col("CardID"), col("source_position"),
              col("lon").as("Next_longitude"), col("lat").as("Next_latitude"))

    val busToGeo = namedTransfer.filter(col("to_system") === "BUS").join(busGeo.select(
        col("CardID"), col("tap_ts").as("NextEventTime"),
        col("lon"), col("lat")),
        Seq("CardID", "NextEventTime"), "left").select(col("CardID"), col("source_position"),
              col("lon").as("Next_longitude"), col("lat").as("Next_latitude"))

    val toGeo = metroToGeo.unionByName(busToGeo)

    val enriched = namedTransfer.join(fromGeo, Seq("CardID", "source_position"), "left").join(toGeo, Seq("CardID", "source_position"), "left").persist(StorageLevel.DISK_ONLY)

    // Validate exact event cardinality and spatial coverage BEFORE overwriting old outputs.
    val nEnriched = enriched.count()
    require(nEnriched == nInput,
      "Coordinate joins duplicated or lost transfers: upstream=" + nInput +
      ", joined=" + nEnriched)
    val noGeo = enriched.filter(
      col("Longitude").isNull || col("Latitude").isNull ||
      col("Next_longitude").isNull || col("Next_latitude").isNull).count()
    require(noGeo == 0L,
      "Missing endpoint coordinates for " + noGeo +
      " transfers. Check newdata path, CardID, Time format and inferred alight keys. " +
      "No output folders have been overwritten.")

    // Geo distance is recalculated ONLY as a diagnostic, not a new transfer filter.
    // Different bus boarding coordinates from raw GPS mapping may cause differences.
    def haversineM2(lat1: Column, lon1: Column, lat2: Column, lon2: Column): Column = {
      val a = pow(sin((radians(lat2) - radians(lat1)) / lit(2.0)), lit(2.0)) +
        cos(radians(lat1)) * cos(radians(lat2)) *
        pow(sin((radians(lon2) - radians(lon1)) / lit(2.0)), lit(2.0))
      lit(2.0 * 6371000.0) * asin(sqrt(least(lit(1.0), greatest(lit(0.0), a))))
    }
    val geoCheck = enriched.withColumn("recovered_distance_m", haversineM2(
      col("Latitude"), col("Longitude"),
      col("Next_latitude"), col("Next_longitude")))
    val geoMetrics = geoCheck.agg(
      max(abs(col("recovered_distance_m") - col("verified_distance_m"))).as("maximum_distance_difference_m"),
      sum(when(abs(col("recovered_distance_m") - col("verified_distance_m")) > 200.0,
        lit(1L)).otherwise(lit(0L))).as("n_difference_gt_200m")
    ).first()
    println("Coordinate reconstruction: max distance difference (m)=" +
      geoMetrics.get(0) + ", differences >200m=" + geoMetrics.get(1))

    // ---------- 5. EXACT output schema: Transfer_Events_Positions ----------
    // Reuse upstream time-ordered positions; no loss of repeats, no OD-level distinct.
    val events = enriched.select(
      col("CardID"), col("EventTime"), col("NextEventTime"),
      col("source_position").as("Pos"), col("Transfer_Route"),
      col("Longitude"), col("Latitude"), col("Next_longitude"), col("Next_latitude")
    ).persist(StorageLevel.DISK_ONLY)

    val pairs = enriched.select(
      col("CardID"),
      when(col("from_system") === "BUS", lit("巴士")).otherwise(lit("地铁出站")).as("Type"),
      col("from_station").as("Station"),
      col("Longitude"), col("Latitude"),
      col("EventTime").as("Time"),
      when(col("to_system") === "BUS", lit("巴士")).otherwise(lit("地铁入站")).as("next_Type"),
      col("to_station").as("next_Station"),
      col("NextEventTime").as("next_Time"),
      col("Next_longitude"), col("Next_latitude"),
      (col("NextEventTime").cast("long") - col("EventTime").cast("long")).as("time_diff_seconds")
    )
    val acceptedSpatialPairs = pairs.join(
      enriched.select("CardID", "source_position", "verified_distance_m"), Seq("CardID"), "inner")
    // Recreate spatial diagnostic correctly WITHOUT joining multiple same-card events.
    val spatialDiagnostics = enriched.select(
      col("CardID"),
      when(col("from_system") === "BUS", lit("巴士")).otherwise(lit("地铁出站")).as("Type"),
      col("from_station").as("Station"),
      col("Longitude"), col("Latitude"),
      col("EventTime").as("Time"),
      when(col("to_system") === "BUS", lit("巴士")).otherwise(lit("地铁入站")).as("next_Type"),
      col("to_station").as("next_Station"),
      col("NextEventTime").as("next_Time"),
      col("Next_longitude"), col("Next_latitude"),
      (col("NextEventTime").cast("long") - col("EventTime").cast("long")).as("time_diff_seconds"),
      col("verified_distance_m").as("distance_m")
    )

    // ---------- 6. Chronological TR: ARRAY<STRING> ----------
    val trajectories = events.groupBy("CardID").agg(sort_array(collect_list(struct(
         col("Pos"), col("Transfer_Route")))).as("_events")).withColumn("TR", expr("transform(_events, x -> x.Transfer_Route)")).select("CardID", "TR")

    // ---------- 7. Complete OD summary U: ALL events, ALL positions ----------
    val odFrequency = events.groupBy("CardID", "Transfer_Route").agg(count(lit(1)).as("freq"),
           sort_array(collect_list(col("Pos"))).as("Positions")).persist(StorageLevel.DISK_ONLY)

    val summaries = odFrequency.groupBy("CardID").agg(sort_array(collect_list(struct(
        (-col("freq")).as("sortKey"), col("Transfer_Route").as("od"),
        col("freq").as("freq"), col("Positions").as("Pos")
      ))).as("_sorted")).withColumn("U", expr(
        "transform(_sorted, x -> named_struct('od', x.od, 'freq', x.freq, 'Pos', x.Pos))")).select("CardID", "U")

    val readableU = summaries.withColumn("Transfer_Trajectory", expr(
      "concat_ws(',', transform(U, x -> concat(x.od, ':', cast(x.freq as string), ':', " +
      "concat_ws('|', transform(x.Pos, p -> cast(p as string))))))")).select("CardID", "Transfer_Trajectory")

    // ---------- 8. Feature_Vector_F: same per-card cumulative 80% frequency ----------
    val cardWin = Window.partitionBy("CardID")
    val descending = Window.partitionBy("CardID").orderBy(col("freq").desc, col("Transfer_Route").asc)
    val ranked = odFrequency.withColumn("total_freq", sum(col("freq")).over(cardWin)).withColumn("cumulative", sum(col("freq")).over(
        descending.rowsBetween(Window.unboundedPreceding, Window.currentRow))).withColumn("before", col("cumulative") - col("freq")).filter(col("before").cast("double") <
        col("total_freq").cast("double") * lit(featureThreshold)).withColumn("weight", col("freq").cast("double") /
        col("total_freq").cast("double"))

    val features = ranked.groupBy("CardID").agg(sort_array(collect_list(struct(
        (-col("freq")).as("sortKey"), col("Transfer_Route").as("od"),
        col("freq").as("freq"), col("weight").as("weight")
      ))).as("_sorted")).withColumn("F", expr(
        "transform(_sorted, x -> named_struct('od', x.od, 'freq', x.freq, 'weight', x.weight))")).select("CardID", "F")

    // ---------- 9. Spatial clustering CSV outputs: same names and columns ----------
    val odGeoOrder = Window.partitionBy("Transfer_Route").orderBy(col("EventTime").asc, col("CardID").asc, col("Pos").asc)
    val gpsdata = events.withColumn("_rn", row_number().over(odGeoOrder)).filter(col("_rn") === 1).select(
        col("Transfer_Route"), col("Longitude").as("lon1"),
        col("Next_longitude").as("lon2"),
        col("Latitude").as("lat1"), col("Next_latitude").as("lat2"))

    val featureWeight = ranked.groupBy("Transfer_Route").agg(sum(col("freq")).as("weight"))
    val centerPoint = featureWeight.join(gpsdata, Seq("Transfer_Route"), "inner").withColumn("station1", split(col("Transfer_Route"), " -> ").getItem(0)).withColumn("station2", split(col("Transfer_Route"), " -> ").getItem(1)).withColumn("center_lon", (col("lon1") + col("lon2")) / lit(2.0)).withColumn("center_lat", (col("lat1") + col("lat2")) / lit(2.0)).select("station1", "station2", "weight", "lon1", "lat1", "lon2", "lat2",
              "center_lon", "center_lat")
    val globalCounts = odFrequency.groupBy("Transfer_Route").agg(sum(col("freq")).as("GlobalFrequency"))

    // ---------- 10. Validate before ANY overwrite ----------
    val nEvents = events.count()
    val nSummedEvents = odFrequency.agg(sum(col("freq"))).first().getLong(0)
    val badPos = odFrequency.filter(size(col("Positions")) =!= col("freq")).count()
    val missingFeature = summaries.select("CardID").join(features.select("CardID"), Seq("CardID"), "left_anti").count()
    val nCards = trajectories.count()
    val nU = summaries.count()
    val nCenterOds = centerPoint.count()
    val nFeatureOds = featureWeight.count()
    require(nInput == nEvents, "Transfer events lost during coordinate reconstruction")
    require(nEvents == nSummedEvents, "Frequency total differs from transfer event count")
    require(badPos == 0L, "OD frequency != length of position list")
    require(missingFeature == 0L, "Some TR cards have no feature vector F")
    require(nCards == nU, "TR and U card counts differ")
    require(nCenterOds == nFeatureOds, "Some F ODs have no spatial coordinates")
    println("Validation OK: transfers=" + nEvents + ", TR cards=" + nCards +
      ", U cards=" + nU + ", F ODs=" + nFeatureOds)

    // ---------- 11. Write: SAME old directory names/schemas ----------
    pairs.write.mode("overwrite").parquet(outRoot + "/filtered_time")
    spatialDiagnostics.write.mode("overwrite").parquet(outRoot + "/filtered_distance")
    events.write.mode("overwrite").parquet(outRoot + "/Transfer_Events_Positions")
    trajectories.write.mode("overwrite").parquet(outRoot + "/Transfer_TR_Chronological")
    trajectories.withColumn("Transfer_Trajectory", concat_ws(",", col("TR"))).select("CardID", "Transfer_Trajectory").write.mode("overwrite").option("header", "true").csv(outRoot + "/Transfer_Trajectory_WithoutWeight_debug")
    summaries.write.mode("overwrite").parquet(outRoot + "/Complete_OD_Summary_U")
    readableU.write.mode("overwrite").option("header", "true").csv(outRoot + "/Transfer_Trajectory_Freq_Pos_CSV")
    features.write.mode("overwrite").parquet(outRoot + "/Feature_Vector_F")
    features.withColumn("Feature_Vector", expr(
      "concat_ws(',', transform(F, x -> concat(x.od, ':', cast(x.freq as string))))")).select("CardID", "Feature_Vector").write.mode("overwrite").option("header", "true").csv(outRoot + "/Feature_Vector_CSV")
    gpsdata.write.mode("overwrite").option("header", "true").csv(outRoot + "/TransferWithLngAndLa")
    centerPoint.write.mode("overwrite").option("header", "true").csv(outRoot + "/center_point")
    globalCounts.write.mode("overwrite").parquet(outRoot + "/Global_OD_Counts")

    println("DONE: " + outRoot)
    println("NOTE: downstream U, F, TR, center_point schemas and paths unchanged.")
  }
}

TrajectorySummaryBuilder.run(spark)
