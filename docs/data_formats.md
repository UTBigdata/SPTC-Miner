# Input and output data formats

This document distinguishes raw examples from the Parquet inputs required by the revised miner. Data fields are derived from the supplied revised source; additional data columns are omitted for brevity.

## Supplied original sample CSVs (unchanged content)

- `sample_data/original/smart_card_records.csv` — smart-card records from the older sample package; original header and values use a legacy Chinese encoding (typically GBK/GB18030). Check encoding before parsing.
- `sample_data/original/bus_gps_records.csv` — **semicolon-separated** bus-GPS rows, fields `idx`, `opath`, `linenumber`, `lng`, `lat`, `time`, `pgeom`.
- `sample_data/original/transfer_feature_vectors.csv` — per-card frequency-only `Feature_Vector` with `groupID`/`clusterID`, originally supplied in a legacy Chinese encoding. It does not include `Pos` or reconstruct the original chronological `TR`.

All three files retain source byte content (apart from the English release filenames). Station names and schema-specific Chinese words are intentionally left as input data; source calculations depend on some Chinese raw column names and transaction labels.

## Synthetic standalone examples

- `sample_data/synthetic/chronological_transfer_sequences.csv` — columns `CardID`, `Transfer_Trajectory`; `Transfer_Trajectory` is a comma-separated chronological list of directed OD event strings.
- `sample_data/synthetic/complete_od_summaries.jsonl` — `CardID`, `TR` array, and complete `U` array with `od`, `freq`, and 1-based sorted `Pos`. It demonstrates how repeated event types retain separate original positions.

`examples/run_local_demo.py` reads the synthetic JSONL and directly invokes the authentic local mining implementation. The JSONL file is a human-readable example, not an HDFS Parquet directory.

## Revised Spark Parquet contracts

| Dataset directory | Relevant fields | Explanation |
|---|---|---|
| `Transfer_TR_Chronological` | `CardID: string`, `TR: array<string>` | Original time-sorted transfer OD sequence; repeats retained |
| `Complete_OD_Summary_U` | `CardID: string`, `U: array<struct<od:string,freq:long,Pos:array<int>>>` (numeric physical types may vary) | Entire summary with every original event index |
| `Feature_Vector_F` | `CardID`, `F` | Compressed high-frequency OD features for clustering only |
| `center_point` | source/destination OD station fields; numeric weight and centroid longitude/latitude | Clustering input CSV; see `clustering/parser.py` for positional column expectations |
| `repartition_complete_U` | `partitionID`, `CardID`, `U`, `F`, `groupID`, `clusterID` | Full original summary copied into relevant logical/physical partitions |
| `Local_TPGI_Postings` | `partitionID: int`, `CardID: string`, `od: string`, `Frequency: long`, `Positions: array<int>` | Flat position-preserving postings used by default in the miner |
| `Global_OD_Counts` | `Transfer_Route: string`, `GlobalFrequency: long` | Globally deduplicated counts prior to trajectory replication |
| `Global_Topology_Adjacency` | `A: string`, `Neighbors: array<string>` | Broadcast-only directed graph adjacency, no passenger support data |

**Validity checks:** Each OD's original `Pos` array is strictly increasing, `Frequency = size(Positions)`, and each (`partitionID`, `CardID`) row describes a complete original trajectory. The miner treats `partitionID` as an integer in `0..numPartitions-1`. A stationary graph edge is represented by a directed string containing literal ` -> `.

## Final output

The primary miner writes a Spark CSV or Parquet dataset with `pattern`, `total_support`, `is_single_group`, `group_count`, and `pattern_length`. `total_support` is global one-off event-level support. The output path is a **Spark directory**, not a single local `.csv` file. Baselines use other columns and support semantics; see `docs/baselines.md`.
