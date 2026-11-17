# Processing pipeline and code map

This map follows the **revised SPTC-Miner manuscript and modular implementation**. The source scripts retain their implementation and data-specific schemas; public host/path placeholders and English comments are used. Files are independently launched (the Scala modules each define a named object and call its `run(spark)` method).

| Paper / operation | Entry point | Main output / role |
|---|---|---|
| Section 4.1: standardize source data | `pipeline/01_preprocessing/01_normalize_ticket_and_gps.scala` to `05_clean_station_names.scala` | Cleaned transaction, GPS, stop, and merged event data |
| Section 4.1: infer bus alighting and detect cross-system transfers | `pipeline/02_alighting_inference/bus_alighting_inference.scala` | `inferred_bus_alighting`, `transfer_events` (upstream transfer detection) |
| Sections 3 and 4.1–4.2: build ordered TR, complete U, original Positions, compressed F | `pipeline/03_position_summaries/trajectory_summary_builder.scala` | `Transfer_TR_Chronological`, `Complete_OD_Summary_U`, `Feature_Vector_F`, `center_point`, `Global_OD_Counts` |
| Section 4.2: two-stage spatial clustering | `scripts/run_clustering.py`; `src/sptc_miner/clustering/` | OD center-label mapping as Spark CSV output |
| Section 4.2: multiple mapping of full trajectories | `pipeline/05_label_remapping/trajectory_label_mapper.scala` | `repartition_complete_U`, local OD counts/postings, global adjacency |
| Additional full-U owner coverage included in provided revision code | `pipeline/06_partition_coverage/partition_coverage_repair.scala` | `CoverageFixed_m64_sup200/{repartition_complete_U,Local_TPGI_Postings,Global_Topology_Adjacency,...}` |
| Sections 4.3–4.4: position-aware local TPGI and constrained growth | `scripts/run_mining.py`; `src/sptc_miner/mining/{index,topology,growth,support,reducers,cli}.py` | Global pattern CSV/Parquet, deduplicated by original `CardID` |

## Key algorithm distinctions

**Sequence:** `TR` is a chronologically ordered array of complete, directed transfer ODs. A transfer OD may appear repeatedly within one original sequence.

**Complete summary:** `U` contains all distinct ODs, their `freq`, and their original strictly increasing `Pos` arrays; `freq = len(Pos)`. Sorting `U` by descending frequency does **not** reorder the original event positions.

**Feature vector:** The code's cumulative-frequency threshold is `featureThreshold = 0.80` for clustering. Full `U` is carried unchanged with each remapped trajectory, rather than mining truncated feature vectors.

**Multiple mapping:** Features identify relevant spatial partitions; a trajectory may be copied to several partitions, with only one copy per (`CardID`, `partitionID`). An owner-coverage pass may add additional full-U copies. Do not confuse this with assigning each event independently.

**TPGI and support:** A partition-local transfer graph uses locally frequent OD edges; `I[od][CardID]` stores frequency and original positions. A separate global adjacency index supports path reachability only. One-off support counts maximal valid ordered matches without reusing an event position. No event-position gap/time-window limit is imposed by the `--minGap/--maxGap` graph-hop parameters.

**Aggregation:** Local `(pattern, CardID)` contributions are deduplicated across replicated partitions before summing global event-level support. This is not the standard MLlib sequence-level support.

**Completeness:** The revised PDF explicitly treats 80% vs 100% equality on the reported dataset as experimental evidence, not a universal theorem. The sufficient local-support-coverage condition must hold for a guaranteed pattern to survive local pruning.
