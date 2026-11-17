'Two-stage weighted spatial OD clustering with full feature coverage.'

import math
import numpy as np
from sklearn.metrics.pairwise import euclidean_distances

def two_stage_clustering(data_list, groups_num, clusters_num):
    'Two-stage spatial clustering: weighted longitude grouping followed by local compact clusters.'
  
    data_list = np.array(data_list, dtype=object)
  
    sorted_data = data_list[data_list[:, 7].astype(float).argsort()]
    converted_weight = sorted_data[:, 2].astype(float)
    ave_group_weight = math.ceil(sum(converted_weight) / groups_num)

  
    groups = []
    current_group_weight = 0
    group_id = 1
    sorted_data_list = sorted_data.tolist()
    for row in sorted_data_list:
        if current_group_weight < ave_group_weight:
            current_group_weight += float(row[2])
        else:
            group_id += 1
            current_group_weight = float(row[2])
        row.append(group_id)
        groups.append(row)
    groups = np.array(groups, dtype=object)

  
    group_weight = {}
    for point in groups:
        gid_int = int(point[9])
        if gid_int not in group_weight:
            group_weight[gid_int] = 0
        group_weight[gid_int] += float(point[2])

    clusters = []

  
    remaining_data = groups.copy()
    real_group_ids = sorted(group_weight.keys())

    for idx, gid in enumerate(real_group_ids):

        mask = remaining_data[:, 9].astype(int) == gid
        current_group_data = remaining_data[mask]

        current_group_weight = group_weight[gid]
        ave_weight = math.ceil(current_group_weight / clusters_num)
        current_cluster_weight = 0
        group_centers = []  # centers for coverage completion if records remain

        for j in range(clusters_num):
            if len(current_group_data) == 0:
                break

          
            cluster_center_index = np.argsort(current_group_data[:, 8].astype(float), kind='mergesort')[0]
            cluster_center = current_group_data[cluster_center_index]
            current_group_data = np.delete(current_group_data, cluster_center_index, axis=0)

            # Crucial fix: the chosen center is a real OD and MUST also be output.
            cluster_id = idx * clusters_num + j + 1
            cluster_center_list = cluster_center.tolist()
            cluster_center_list.append(cluster_id)
            clusters.append(cluster_center_list)
            group_centers.append((float(cluster_center[7]), float(cluster_center[8]), cluster_id))

            if len(current_group_data) == 0:
                break

            distances = euclidean_distances(current_group_data[:, 7:9].astype(float), [cluster_center[7:9].astype(float)])
            converted_weight_group = current_group_data[:, 2].astype(float)

            if sum(converted_weight_group) < ave_weight:
                for _ in range(len(distances)):
                    near_indice = np.argsort(distances.flatten())[0]
                    cluster_id = idx * clusters_num + j + 1
                    cluster_point = current_group_data[near_indice].tolist()
                    cluster_point.append(cluster_id)
                    clusters.append(cluster_point)
                    current_group_data = np.delete(current_group_data, near_indice, axis=0)
                    distances = np.delete(distances, near_indice, axis=0)
                    break
            else:
                for _ in range(len(distances)):
                    near_indice = np.argsort(distances.flatten())[0]
                    if current_cluster_weight < ave_weight:
                        cluster_id = idx * clusters_num + j + 1
                        cluster_point = current_group_data[near_indice].tolist()
                        cluster_point.append(cluster_id)
                        clusters.append(cluster_point)
                        current_cluster_weight += float(current_group_data[near_indice][2])
                        current_group_data = np.delete(current_group_data, near_indice, axis=0)
                        distances = np.delete(distances, near_indice, axis=0)
                    else:
                        current_cluster_weight = 0
                        break

        # Preserve all OD feature items. The original code could leave items
        # unassigned after clusters_num iterations, then discard them here.
        # Assign only those leftovers to the nearest selected center in this group.
        if len(current_group_data) > 0:
            if not group_centers:
                raise RuntimeError(f"group {gid}: no cluster center generated")
            for point in current_group_data:
                x, y = float(point[7]), float(point[8])
                nearest = min(group_centers,
                              key=lambda c: (x - c[0])**2 + (y - c[1])**2)
                covered = point.tolist()
                covered.append(nearest[2])
                clusters.append(covered)

      
        remaining_data = remaining_data[remaining_data[:, 9].astype(int) != gid]

    return clusters
