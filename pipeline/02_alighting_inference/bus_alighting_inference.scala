// Input/output and processing details are described in the English documentation.
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/** Processing module; see docs/processing_pipeline.md for the full description. */
object BusAlightingInference {
  def run(spark: SparkSession): Unit = {
    import spark.implicits._

    spark.conf.set("spark.sql.session.timeZone", "Asia/Shanghai")
    spark.conf.set("spark.sql.adaptive.enabled", "true")

    // --------------- 0. Paths / thresholds ---------------
    val base = "hdfs://namenode.example.org:9000/project/DATA/201901"
    val ticketPath = s"$base/newdata"          // CardID,Time,Type,station,line,carID,longitude,latitude
    val gpsPath    = s"$base/busWithstation"   // idx,linenumber,name,longitude,latitude,time,direction,position
    val outBase    = s"$base/bus_alighting_transfer_MP2012"

    val BOARD_MATCH_SEC       = 120L           // keep original IC/GPS boarding matching rule
    val TRIP_GAP_SEC          = 45L * 60L      // heuristic run boundary; unchanged
    val MAX_RIDE_SEC          = 3L * 3600L     // hard upper bound for a bus ride (data-adaptation)

    // Eq. (1)-(2) of Munizaga & Palma (2012): Tg_i = ride_sec_i + fw * walk_m_i / sw.
    val ALIGHT_WALK_RADIUS_M  = 1000.0         // d=1000m in their study; strict distance < d
    val WALK_SPEED_MPS        = 1.25           // sw: user-calibrated assumption, NOT a reported paper constant
    val WALK_PENALTY_FW       = 2.0            // fw: user-calibrated assumption, NOT a reported paper constant
    // Paper describes an initial GPS search window, doubled when boundary becomes active.
    // Numerical window length and edge definition are NOT provided in the paper.
    val BASE_RIDE_SEARCH_SEC  = 90L * 60L      // local adaptable search window, not a paper parameter
    val SEARCH_EDGE_SEC       = 10L * 60L      // extension triggered when base optimum is near its boundary
    val ENABLE_DAILY_CLOSURE  = true           // last transaction destination near first boarding of same day

    // Original SPTC-Miner cross-system TRANSFER definition (independent of alighting inference).
    val TRANSFER_MAX_SEC      = 50L * 60L
    val TRANSFER_MAX_M        = 1500.0
    // Paper Section 4.5's 30-minute activity rule is recorded as an annotation only.
    val PAPER_ACTIVITY_GAP_SEC = 30L * 60L

    require(WALK_SPEED_MPS > 0.0 && WALK_PENALTY_FW >= 0.0 && ALIGHT_WALK_RADIUS_M > 0.0)
    require(BASE_RIDE_SEARCH_SEC > SEARCH_EDGE_SEC && 2L * BASE_RIDE_SEARCH_SEC <= MAX_RIDE_SEC)

    // Distances in meters for WGS84 GPS points.
    def haversineM(lat1: Column, lon1: Column, lat2: Column, lon2: Column): Column = {
      val a = pow(sin((radians(lat2) - radians(lat1)) / lit(2.0)), lit(2.0)) +
        cos(radians(lat1)) * cos(radians(lat2)) *
        pow(sin((radians(lon2) - radians(lon1)) / lit(2.0)), lit(2.0))
      lit(2.0 * 6371000.0) * asin(sqrt(least(lit(1.0), greatest(lit(0.0), a))))
    }

    def readCsv(path: String): DataFrame =spark.read.option("header", "true").option("encoding", "UTF-8").option("inferSchema", "false").csv(path)

    // --------------- 1. IC-card events ---------------
    val allTaps = readCsv(ticketPath).select(
        trim(col("CardID")).as("CardID"),
        trim(col("Time")).as("Time"),
        trim(col("Type")).as("Type"),
        trim(col("station")).as("station"),
        trim(col("line")).as("line"),
        regexp_replace(trim(col("carID")), "\\s+", "").as("carID"),
        col("longitude").cast("double").as("lon"),
        col("latitude").cast("double").as("lat")
      ).withColumn("tap_ts", coalesce(
        to_timestamp(col("Time"), "yyyyMMddHHmmss"),
        to_timestamp(col("Time"), "yyyy-MM-dd HH:mm:ss")
      )).withColumn("type_lc", lower(col("Type"))).withColumn("kind",
        when(col("type_lc").rlike("巴士|公交|bus"), lit("BUS_IN"))
        .when(col("type_lc").rlike("地铁|metro") && col("type_lc").rlike("进站|入站|entry"), lit("METRO_IN"))
        .when(col("type_lc").rlike("地铁|metro") && col("type_lc").rlike("出站|出闸|exit"), lit("METRO_OUT"))
      ).filter(col("CardID").isNotNull && col("tap_ts").isNotNull &&
              col("kind").isNotNull && col("lat").isNotNull && col("lon").isNotNull).dropDuplicates(Seq("CardID", "Time", "Type", "station", "line", "carID")).withColumn("event_id", sha2(concat_ws("\u0001",
        col("CardID"), col("Time"), col("Type"), col("station"),
        col("line"), col("carID")), 256)).drop("type_lc")

    // Daily trip chains: the paper closes the last transaction of each day at the day's
    // first boarding. Do not link the day's final tap to the following calendar day.
    val tapsBase = allTaps.withColumn("tap_date", to_date(col("tap_ts")))
    val dayW = Window.partitionBy("CardID", "tap_date").orderBy(col("tap_ts").asc, col("event_id").asc)
    val dayWFull = dayW.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    val dayPartW = Window.partitionBy("CardID", "tap_date")

    val taps = tapsBase.withColumn("next_ts", lead(col("tap_ts"), 1).over(dayW)).withColumn("next_kind", lead(col("kind"), 1).over(dayW)).withColumn("next_station", lead(col("station"), 1).over(dayW)).withColumn("next_lat", lead(col("lat"), 1).over(dayW)).withColumn("next_lon", lead(col("lon"), 1).over(dayW)).withColumn("next_line", lead(col("line"), 1).over(dayW)).withColumn("next_event_id", lead(col("event_id"), 1).over(dayW)).withColumn("prev_kind", lag(col("kind"), 1).over(dayW)).withColumn("prev_ts", lag(col("tap_ts"), 1).over(dayW)).withColumn("day_event_count", count(lit(1)).over(dayPartW)).withColumn("first_day_kind", first(col("kind")).over(dayWFull)).withColumn("first_day_station", first(col("station")).over(dayWFull)).withColumn("first_day_lat", first(col("lat")).over(dayWFull)).withColumn("first_day_lon", first(col("lon")).over(dayWFull)).persist(StorageLevel.DISK_ONLY)

    val busTaps = taps.filter(col("kind") === "BUS_IN" &&
      col("carID").isNotNull && col("line").isNotNull)

    // --------------- 2. Bus stop visits and run segmentation ---------------
    // busWithstation already maps bus GPS points to static stops; it may contain
    // many consecutive GPS points for the same stop.
    val rawStops = readCsv(gpsPath).select(
        regexp_replace(trim(col("idx")), "\\s+", "").as("gps_car"),
        trim(col("linenumber")).as("gps_line"),
        regexp_replace(trim(col("name")), "[①②③④⑤⑥⑦⑧⑨⑩]", "").as("gps_station"),
        col("direction").cast("int").as("gps_direction"),
        col("position").cast("int").as("gps_position"),
        col("longitude").cast("double").as("gps_lon"),
        col("latitude").cast("double").as("gps_lat"),
        coalesce(to_timestamp(col("time"), "yy-MM-dd HH:mm:ss"),
                 to_timestamp(col("time"), "yyyy-MM-dd HH:mm:ss")).as("gps_ts")
      ).filter(col("gps_car").isNotNull && col("gps_line").isNotNull &&
              col("gps_ts").isNotNull && col("gps_direction").isNotNull &&
              col("gps_position") > 0 && col("gps_lat").isNotNull &&
              col("gps_lon").isNotNull).dropDuplicates(Seq("gps_car", "gps_line", "gps_direction", "gps_position", "gps_ts"))

    // A trip boundary is indicated by direction change, stop-index reset, or GPS gap.
    // This is a fallback heuristic because section 5 of the Word document drops 'opath'.
    val busW = Window.partitionBy("gps_car", "gps_line").orderBy(col("gps_ts"), col("gps_direction"), col("gps_position"))
    val segmentedStops = rawStops.withColumn("prev_gps_ts", lag(col("gps_ts"), 1).over(busW)).withColumn("prev_dir", lag(col("gps_direction"), 1).over(busW)).withColumn("prev_pos", lag(col("gps_position"), 1).over(busW)).withColumn("run_start", when(
        col("prev_gps_ts").isNull ||
        col("prev_dir") =!= col("gps_direction") ||
        col("gps_position") < col("prev_pos") ||
        (col("gps_ts").cast("long") - col("prev_gps_ts").cast("long")) > TRIP_GAP_SEC,
        lit(1L)).otherwise(lit(0L))).withColumn("trip_no", sum(col("run_start")).over(
        busW.rowsBetween(Window.unboundedPreceding, Window.currentRow)))

    val stopPass = segmentedStops.withColumn("pass_rn", row_number().over(
        Window.partitionBy("gps_car", "gps_line", "trip_no", "gps_direction", "gps_position").orderBy(col("gps_ts").asc, col("gps_station").asc))).filter(col("pass_rn") === 1).drop("prev_gps_ts", "prev_dir", "prev_pos", "run_start", "pass_rn").persist(StorageLevel.DISK_ONLY)

    // --------------- 3. Locate actual bus boarding visit ---------------
    // Critical fix compared with the Word script, section 7:
    // join on BOTH vehicle (carID=idx) and route (line=linenumber), not only time+route.
    // 2-minute buckets avoid a broad vehicle/time range join.
    // Use every GPS point for boarding (including dwell points), not just the first
    // arrival of a stop; otherwise a boarding late in the dwell can be missed.
    val gpsBuckets = segmentedStops.withColumn("time_bucket", floor(col("gps_ts").cast("long") / lit(BOARD_MATCH_SEC)).cast("long"))
    val boardingBuckets = busTaps.withColumn("bucket0", floor(col("tap_ts").cast("long") / lit(BOARD_MATCH_SEC)).cast("long")).withColumn("time_bucket", explode(array(
        col("bucket0") - lit(1L), col("bucket0"), col("bucket0") + lit(1L))))

    val possibleBoard = boardingBuckets.alias("t").join(gpsBuckets.alias("g"),
        col("t.carID") === col("g.gps_car") &&
        col("t.line") === col("g.gps_line") &&
        col("t.time_bucket") === col("g.time_bucket"), "inner").select(
        col("t.event_id").as("event_id"),
        col("t.tap_ts").as("tap_ts"),
        col("t.lat").as("raw_lat"), col("t.lon").as("raw_lon"),
        col("g.gps_ts").as("board_gps_ts"),
        col("g.gps_station").as("board_station"),
        col("g.gps_lat").as("board_lat"),
        col("g.gps_lon").as("board_lon"),
        col("g.gps_car").as("gps_car"),
        col("g.gps_line").as("gps_line"),
        col("g.gps_direction").as("board_direction"),
        col("g.gps_position").as("board_position"),
        col("g.trip_no").as("trip_no")
      ).withColumn("board_sec_error",
        abs(col("tap_ts").cast("long") - col("board_gps_ts").cast("long"))).filter(col("board_sec_error") <= BOARD_MATCH_SEC).withColumn("raw_to_gps_m", haversineM(
        col("raw_lat"), col("raw_lon"), col("board_lat"), col("board_lon")))

    val verifiedBoard = possibleBoard.withColumn("match_rn", row_number().over(
        Window.partitionBy("event_id").orderBy(col("board_sec_error").asc,
                   col("raw_to_gps_m").asc, col("board_position").asc))).filter(col("match_rn") === 1).drop("match_rn", "tap_ts", "raw_lat", "raw_lon").persist(StorageLevel.DISK_ONLY)

    // --------------- 4. Munizaga-Palma bus alighting (2012), Eq. (1)-(2) ---------------
    // For ordinary legs, the very next IC-card observation is the spatial anchor, regardless
    // of its mode (the paper uses the next observed transaction). The search endpoint is
    // its actual timestamp; NO 50-min transfer threshold is imposed on bus alighting.
    // For a day's final transaction, if there were >=2 taps and the first was an observed
    // boarding, use the first day's boarding coordinates as the closure target. The closure
    // is an inferred DESTINATION only and MUST NOT be used to create a transfer OD.
    val alightingAnchors = busTaps.alias("t").join(verifiedBoard.alias("v"), Seq("event_id"), "inner").withColumn("anchor_type",
        when(col("next_ts").isNotNull && col("next_ts") > col("tap_ts") &&
             col("next_lat").isNotNull && col("next_lon").isNotNull, lit("NEXT_TAP")).when(lit(ENABLE_DAILY_CLOSURE) && col("next_ts").isNull &&
              col("day_event_count") > 1L &&
              col("first_day_kind").isin("BUS_IN", "METRO_IN"), lit("DAY_CLOSURE"))).filter(col("anchor_type").isNotNull).withColumn("anchor_lat", when(col("anchor_type") === "NEXT_TAP", col("next_lat")).otherwise(col("first_day_lat"))).withColumn("anchor_lon", when(col("anchor_type") === "NEXT_TAP", col("next_lon")).otherwise(col("first_day_lon"))).withColumn("anchor_station", when(col("anchor_type") === "NEXT_TAP", col("next_station")).otherwise(col("first_day_station"))).withColumn("search_end_ts", when(col("anchor_type") === "NEXT_TAP", col("next_ts")).otherwise(date_add(col("tap_date"), 1).cast("timestamp"))).select(
        col("event_id"), col("CardID"), col("tap_ts").as("board_ts"),
        col("next_ts"), col("next_kind"), col("next_line"), col("next_event_id"),
        col("anchor_type"), col("anchor_station"), col("anchor_lat"), col("anchor_lon"),
        col("search_end_ts"), col("gps_car"), col("gps_line"), col("trip_no"),
        col("board_direction"), col("board_position"), col("board_gps_ts"),
        col("board_station"), col("board_lat"), col("board_lon")
      ).persist(StorageLevel.DISK_ONLY)

    // busWithstation offers stop-matched GPS samples. Candidate points are the vehicle's
    // downstream stop passages from the same vehicle/route/run/direction as the boarding.
    // The coarse 0.03-degree prefilter only limits joins; the true criterion is Haversine <1km.
    val candidatePool = alightingAnchors.alias("b").join(stopPass.alias("s"),
        col("b.gps_car") === col("s.gps_car") &&
        col("b.gps_line") === col("s.gps_line") &&
        col("b.trip_no") === col("s.trip_no") &&
        col("b.board_direction") === col("s.gps_direction") &&
        col("s.gps_position") > col("b.board_position") &&
        col("s.gps_ts") > col("b.board_ts") &&
        col("s.gps_ts") > col("b.board_gps_ts") &&
        col("s.gps_ts") <= col("b.search_end_ts") &&
        abs(col("s.gps_lat") - col("b.anchor_lat")) <= lit(0.03) &&
        abs(col("s.gps_lon") - col("b.anchor_lon")) <= lit(0.03), "inner").select(
        col("b.event_id").as("event_id"), col("b.CardID").as("CardID"),
        col("b.board_ts").as("board_ts"),
        col("b.board_station").as("board_station"),
        col("b.board_position").as("board_position"),
        col("b.board_direction").as("direction"),
        col("b.gps_line").as("line"), col("b.gps_car").as("carID"),
        col("b.next_ts").as("next_ts"), col("b.next_kind").as("next_kind"),
        col("b.next_line").as("next_line"),
        col("b.next_event_id").as("next_event_id"),
        col("b.anchor_type").as("anchor_type"),
        col("b.anchor_station").as("next_station"),
        col("b.anchor_lat").as("next_lat"), col("b.anchor_lon").as("next_lon"),
        col("s.gps_ts").as("alight_ts"),
        col("s.gps_station").as("alight_station"),
        col("s.gps_position").as("alight_position"),
        col("s.gps_lat").as("alight_lat"), col("s.gps_lon").as("alight_lon")
      ).withColumn("ride_sec", col("alight_ts").cast("long") - col("board_ts").cast("long")).filter(col("ride_sec") > 0L && col("ride_sec") <= MAX_RIDE_SEC).withColumn("walk_m", haversineM(
        col("alight_lat"), col("alight_lon"), col("next_lat"), col("next_lon"))).filter(col("walk_m") < ALIGHT_WALK_RADIUS_M).withColumn("walk_time_sec", col("walk_m") / lit(WALK_SPEED_MPS)).withColumn("generalized_time_sec", col("ride_sec") +
        lit(WALK_PENALTY_FW) * col("walk_time_sec")).withColumn("to_next_sec", when(col("anchor_type") === "NEXT_TAP",
        col("next_ts").cast("long") - col("alight_ts").cast("long")).otherwise(lit(null).cast("long"))).persist(StorageLevel.DISK_ONLY)

    // The paper describes expanding the search window when the original time limit is
    // active. With no route-specific parameters in current inputs, use a documented
    // 90-min -> 180-min fallback. If the base optimum lies in the last 10 minutes of
    // the base window, allow expanded candidates; otherwise keep base candidates.
    val baseBestRide = candidatePool.filter(col("ride_sec") <= BASE_RIDE_SEARCH_SEC).withColumn("base_rank", row_number().over(
        Window.partitionBy("event_id").orderBy(
          col("generalized_time_sec").asc, col("ride_sec").asc,
          col("walk_m").asc, col("alight_ts").asc, col("alight_station").asc))).filter(col("base_rank") === 1).select(col("event_id"), col("ride_sec").as("base_best_ride_sec"))

    val feasibleCandidates = candidatePool.join(baseBestRide, Seq("event_id"), "left").withColumn("expanded_search",
        col("base_best_ride_sec").isNull ||
        col("base_best_ride_sec") >= lit(BASE_RIDE_SEARCH_SEC - SEARCH_EDGE_SEC)).filter(col("ride_sec") <= BASE_RIDE_SEARCH_SEC || col("expanded_search"))

    // In contrast with the old nearest-distance sorting, first minimize Eq. (1).
    val inferredAlighting = feasibleCandidates.withColumn("num_feasible_stops", count(lit(1)).over(Window.partitionBy("event_id"))).withColumn("candidate_rn", row_number().over(
        Window.partitionBy("event_id").orderBy(
          col("generalized_time_sec").asc, col("ride_sec").asc,
          col("walk_m").asc, col("alight_ts").asc, col("alight_station").asc))).filter(col("candidate_rn") === 1).drop("candidate_rn", "base_best_ride_sec") .withColumn("paper_postprocess",when(col("anchor_type") === "DAY_CLOSURE", lit("DESTINATION_CLOSURE")).when(col("to_next_sec") > PAPER_ACTIVITY_GAP_SEC, lit("DESTINATION_ACTIVITY")).when(col("next_kind") === "BUS_IN" && col("next_line") === col("line"),
          lit("DESTINATION_SAME_ROUTE")).when(col("next_kind").isin("BUS_IN", "METRO_IN"), lit("POSSIBLE_TRANSFER")).otherwise(lit("UNCLASSIFIED"))).persist(StorageLevel.DISK_ONLY)

    // Auditable status for EVERY original bus boarding, including failed estimates.
    val busAlightingStatus = busTaps.select("event_id", "CardID", "tap_ts", "line", "carID").join(verifiedBoard.select(col("event_id"),
        lit(1).as("has_gps_board_match")), Seq("event_id"), "left").join(alightingAnchors.select(col("event_id"),
        col("anchor_type")), Seq("event_id"), "left").join(inferredAlighting.select(col("event_id"),
        col("alight_station"), col("generalized_time_sec"),
        col("walk_m"), col("num_feasible_stops")), Seq("event_id"), "left").withColumn("estimation_status",
        when(col("has_gps_board_match").isNull, lit("NO_GPS_BOARD_MATCH")).when(col("anchor_type").isNull, lit("NO_USABLE_NEXT_TAP_OR_DAILY_CLOSURE")).when(col("alight_station").isNull, lit("NO_FEASIBLE_DOWNSTREAM_STOP_WITHIN_1000M")).otherwise(lit("SUCCESS"))).drop("has_gps_board_match")

    // --------------- 5. Build observed/inferred point stream ---------------
    // Only GPS-verified bus boardings are emitted, to avoid prior route/time-only join errors.
    val busStart = busTaps.alias("t").join(verifiedBoard.alias("v"), Seq("event_id"), "inner").select(
        col("CardID"), col("tap_ts").as("point_ts"),
        lit("BUS").as("system"), lit("START").as("role"),
        col("board_station").as("point_station"),
        col("board_lat").as("point_lat"), col("board_lon").as("point_lon"),
        col("event_id").as("point_id"),
        lit(null).cast("string").as("expected_next_start_id")
      )

    // Exclude paper's daily-closure estimates from transfers: no observed next boarding.
    // Also require the subsequent next swipe to be a real START event; using a Metro EXIT
    // as an alighting anchor is fine, but it must not fabricate a new bus->metro transfer.
    val busEnd = inferredAlighting.filter(col("anchor_type") === "NEXT_TAP" &&
        col("next_kind").isin("BUS_IN", "METRO_IN")).select(
        col("CardID"), col("alight_ts").as("point_ts"),
        lit("BUS").as("system"), lit("END").as("role"),
        col("alight_station").as("point_station"),
        col("alight_lat").as("point_lat"), col("alight_lon").as("point_lon"),
        concat(col("event_id"), lit("_alight")).as("point_id"),
        col("next_event_id").as("expected_next_start_id")
      )

    val metroStart = taps.filter(col("kind") === "METRO_IN").select(
      col("CardID"), col("tap_ts").as("point_ts"),
      lit("METRO").as("system"), lit("START").as("role"),
      col("station").as("point_station"), col("lat").as("point_lat"),
      col("lon").as("point_lon"), col("event_id").as("point_id"),
      lit(null).cast("string").as("expected_next_start_id")
    )

    // Unlike the paper's origin-only Metro data, metro exits are explicitly recorded here.
    // Accept a Metro OUT only when preceded by a valid Metro IN of the same card/day.
    val metroEnd = taps.filter(col("kind") === "METRO_OUT" && col("prev_kind") === "METRO_IN" &&
              col("prev_ts").isNotNull &&
              (col("tap_ts").cast("long") - col("prev_ts").cast("long")) >= 0L &&
              (col("tap_ts").cast("long") - col("prev_ts").cast("long")) <= MAX_RIDE_SEC).select(
        col("CardID"), col("tap_ts").as("point_ts"),
        lit("METRO").as("system"), lit("END").as("role"),
        col("station").as("point_station"), col("lat").as("point_lat"),
        col("lon").as("point_lon"), col("event_id").as("point_id"),
        col("next_event_id").as("expected_next_start_id")
      )

    val points = busStart.unionByName(busEnd).unionByName(metroStart).unionByName(metroEnd).withColumn("role_sort", when(col("role") === "END", lit(0)).otherwise(lit(1)))

    // --------------- 6. Cross-system transfer ODs: END -> next START ---------------
    val pointW = Window.partitionBy("CardID").orderBy(col("point_ts").asc, col("role_sort").asc, col("point_id").asc)
    val adjacent = points.withColumn("next_role", lead(col("role"), 1).over(pointW)).withColumn("next_system", lead(col("system"), 1).over(pointW)).withColumn("next_point_ts", lead(col("point_ts"), 1).over(pointW)).withColumn("next_point_station", lead(col("point_station"), 1).over(pointW)).withColumn("next_point_lat", lead(col("point_lat"), 1).over(pointW)).withColumn("next_point_lon", lead(col("point_lon"), 1).over(pointW)).withColumn("next_point_id", lead(col("point_id"), 1).over(pointW))

    val transferOD = adjacent.filter(col("role") === "END" && col("next_role") === "START" &&
              col("system") =!= col("next_system") &&
              // Require the actual next IC tap, not an unrelated START after missing records.
              col("expected_next_start_id") === col("next_point_id")).withColumn("delta_sec", col("next_point_ts").cast("long") - col("point_ts").cast("long")).withColumn("distance_m", haversineM(
        col("point_lat"), col("point_lon"), col("next_point_lat"), col("next_point_lon"))).filter(col("delta_sec") >= 0L && col("delta_sec") <= TRANSFER_MAX_SEC &&
              col("distance_m") <= TRANSFER_MAX_M).select(
        col("CardID"), col("point_ts").as("event_time"),
        col("system").as("from_system"), col("point_station").as("from_station"),
        col("next_system").as("to_system"), col("next_point_station").as("to_station"),
        col("next_point_ts").as("next_board_time"),
        (col("delta_sec") / lit(60.0)).as("delta_minutes"),
        col("distance_m"), col("point_id").as("from_point_id"),
        col("next_point_id").as("to_point_id")
      ).withColumn("OD", concat(
        col("from_system"), lit(":"), col("from_station"), lit(" -> "),
        col("to_system"), lit(":"), col("to_station")))

    // Exact chronological sequence: no OD-type distinct(), no frequency sorting.
    val transferRows = transferOD.withColumn("position", row_number().over(
        Window.partitionBy("CardID").orderBy(col("event_time").asc, col("from_point_id").asc, col("to_point_id").asc))).select("CardID", "position", "event_time", "OD", "from_system", "from_station",
        "to_system", "to_station", "next_board_time", "delta_minutes", "distance_m").persist(StorageLevel.DISK_ONLY)

    // An optional nested TR representation consistent with Definition 5.
    val trajectoryTR = transferRows.groupBy("CardID").agg(
      sort_array(collect_list(struct(
        col("position"), col("OD"), col("event_time")))).as("TR"))

    // Separate position lookup only; it does not reorder or deduplicate TR events.
    val odPositions = transferRows.groupBy("CardID", "OD").agg(
      sort_array(collect_list(col("position"))).as("Positions"))

    // --------------- 7. Save (Parquet recommended at ~67 million input taps) ---------------
    inferredAlighting.write.mode("overwrite").parquet(s"$outBase/inferred_bus_alighting")
    transferRows.write.mode("overwrite").parquet(s"$outBase/transfer_events")
    trajectoryTR.write.mode("overwrite").parquet(s"$outBase/TR")
    odPositions.write.mode("overwrite").parquet(s"$outBase/OD_positions")
    busAlightingStatus.write.mode("overwrite").parquet(s"$outBase/bus_alighting_status")

    // Optional for inspection (CSV may generate many part files):
    // transferRows.write.mode("overwrite").option("header", "true")
    //   .csv(s"$outBase/transfer_events_csv")

    println(s"Done. Output: $outBase")
  }
}

BusAlightingInference.run(spark)
