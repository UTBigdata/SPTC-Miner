# SPTC-Miner

SPTC-Miner is a Spark-based frequent transfer pattern mining framework for large-scale bus–metro smart card data. It combines spatial semantic partitioning, topology-constrained pattern growth, and position-aware one-off support to discover frequent passenger transfer patterns.

## 1. Overview

SPTC-Miner contains four main stages:

1. **Transfer Event Sequence Generation**  
   Cleans smart card and bus GPS data, infers bus alighting stops, and generates chronologically ordered transfer event sequences.

2. **Transfer Pattern Compression**  
   Builds complete transfer OD-frequency summaries with original event positions (`Positions`) and weighted features for spatial clustering.

3. **Spatial Semantic Partitioning**  
   Clusters transfer ODs, maps complete passenger summaries to relevant partitions, and repairs coverage of globally frequent ODs.

4. **Topology-Constrained Pattern Mining**  
   Builds local Transfer Pattern Graph Indexes (TPGIs), mines patterns under topology and path-hop gap constraints using one-off event-level support, and aggregates global results.

## 2. Repository Structure

```text
.
├── pipeline/
│   ├── 01_preprocessing/               # Smart card and GPS cleaning/integration
│   ├── 02_alighting_inference/          # Bus alighting and transfer events
│   ├── 03_position_summaries/           # Chronological TR, complete U, Positions, F
│   ├── 05_label_remapping/             # Complete-trajectory partition mapping
│   └── 06_partition_coverage/          # Global frequent-OD coverage repair
├── src/sptc_miner/
│   ├── clustering/                     # Spatial semantic clustering
│   └── mining/                         # TPGI and topology-constrained mining
├── scripts/
│   ├── run_clustering.py
│   ├── run_mining.py
│   └── build_spark_zip.py
├── baselines/
│   ├── mllib/                          # MLlib PrefixSpan
│   ├── ctm/                            # CTM
│   ├── mgfsm/                          # MG-FSM adapter
│   └── tree_miner/                     # Tree-Miner-style baseline
├── sample_data/
│   ├── original/                       # Original sample CSV files
│   └── synthetic/                      # Sequences and summaries with Positions
├── examples/                           # Local demonstration
├── docs/                               # Additional instructions
├── requirements.txt
└── pyproject.toml
```

## 3. Main Modules

| Module | Description |
|---|---|
| `pipeline/` | Prepares transfer event sequences, OD summaries, and partitioned input data. |
| `src/sptc_miner/clustering/` | Performs weighted spatial semantic clustering of transfer ODs. |
| `src/sptc_miner/mining/` | Constructs TPGIs and mines frequent patterns using topology constraints and position-aware one-off support. |
| `scripts/` | Provides entry points for clustering and Spark mining. |
| `baselines/` | Contains comparison methods. |
| `sample_data/` | Provides sample input formats and synthetic data for local testing. |

For the processing steps, see [`docs/processing_pipeline.md`](docs/processing_pipeline.md).  
For input/output formats, see [`docs/data_formats.md`](docs/data_formats.md).

## 4. Running Example

Run the local example using the synthetic data:

```bash
python -m pip install -e .
python examples/run_local_demo.py
```

The example checks topology-constrained pattern growth and one-off event-level support. To run the full Spark pipeline on prepared Parquet data, see [`docs/execution.md`](docs/execution.md).

## 5. Environment Requirements

Recommended environment:

| Component | Version |
|---|---|
| Python | 3.8 or later |
| Apache Spark | 3.x (experiments: 3.1.3) |
| PySpark | Compatible with Spark version |
| Scala | Compatible with Spark version |
| NumPy | 1.20 or later |
| scikit-learn | 1.0 or later |

## 6. Baselines

The `baselines/` directory contains four comparison methods:

- **MLlib:** Spark PrefixSpan.
- **CTM:** K-means clustering and PrefixSpan.
- **MG-FSM:** Input/output adapter for the external MG-FSM implementation.
- **Tree-Miner:** Tree-Miner-style Python implementation.

## 7. Notes

1. The Scala files are written as Spark shell scripts; dataset paths and dependencies must be configured for the target environment.
2. The original sample CSV files illustrate data formats; they do not contain the complete `Positions` required by the revised miner.
3. The synthetic examples are provided for local testing, not for reproducing the full-scale experiments.
4. The full real-world dataset is not publicly released due to privacy and data-sharing restrictions.
5. MG-FSM requires its external implementation, which is not included in this repository.
