'PySpark command-line entry for spatial semantic clustering.'
import sys
from pyspark import SparkConf, SparkContext
from .parser import parse_line
from .algorithm import two_stage_clustering
from .writer import save_output_csv_df

def main():
    if len(sys.argv) != 6:
        print("Usage: spark-submit cluster.py <input_path> <output_path> <groups_num> <clusters_num> <num_partitions>")
        sys.exit(-1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]
    groups_num = int(sys.argv[3])
    clusters_num = int(sys.argv[4])
    num_partitions = int(sys.argv[5])

    conf = SparkConf().setAppName("TwoStageClustering")
    sc = SparkContext(conf=conf)

  
    lines = sc.textFile(input_path, minPartitions=num_partitions)
    header = lines.first()
    data_lines = lines.filter(lambda row: row != header)

  
    data_list = data_lines.map(parse_line).collect()

  
    cluster_list = two_stage_clustering(data_list, groups_num, clusters_num)
    # Mandatory consistency check for downstream code2 (one label per OD).
    if len(cluster_list) != len(data_list):
        raise RuntimeError(
            f"Clustering omitted ODs: input={len(data_list)}, output={len(cluster_list)}")
    print(f"Clustering coverage OK: {len(cluster_list)} / {len(data_list)} OD features")

  
    save_output_csv_df(output_path, cluster_list)

    sc.stop()


if __name__ == "__main__":
    main()
