import sys
from math import sqrt

from numpy import array
from numpy import linalg as LA
import numpy as np

import random

from pyspark import SparkContext
from pyspark.mllib.clustering import KMeans

from collections import defaultdict

N_CLUSTERS = 2
ITER=10

def reservoir_sampler(iter, k):
    import random
    reservoir = list()
    for counter, item in enumerate(iter):
        if counter < k:
            reservoir.append(item)
        else:
            idx = random.randint(0, counter)
            if idx < k:
                reservoir[idx] = item
    if counter < k:
        reservoir = reservoir[:counter]
    return reservoir

def bernoulli_sampler(k, count):
    import random
    rand = random.SystemRandom()
    return rand.random() < float(k) / (float(count))

def distance(vec1, vec2):
    diff = vec2 - vec1
    return np.sqrt(np.dot(diff, diff))

def closest_center(centers, point):
    distances = []
    for idx, center in enumerate(centers):
        distances.append((distance(center, point), idx))
    return min(distances)[1]

class HierarchicalKMeans(object):
    def __init__(self, n_clusters, n_iter, initialization):
        self.n_clusters = n_clusters
        self.n_iter = n_iter
        self.initialization = initialization
        self.n_levels = np.log2(n_clusters)
        assert self.n_levels % 1.0 == 0, "Number of clusters must be a power of 2"
        self.n_levels = int(self.n_levels)

    def _init_random(self, grouped_values, num_groups, k):
        print "Grouped values", grouped_values.collect()

        counts = grouped_values.map(lambda p: (p[0], 1)).reduceByKey(lambda l, r: l + r)
        counts = dict(counts.collect())

        print counts

        sampled = [False] * num_groups
        new_centers = []

        resample = True
        while resample:
            sampled_points = grouped_values.filter(lambda p: bernoulli_sampler(2 * k, counts[p[0]])).groupByKey().collect()

            print sampled_points

            sampled_counts = []
            for group, pt_iter in sampled_points:
                l = list(pt_iter)
                sampled_counts.append(len(l))
                if sampled[group]:
                    continue
                if len(l) == k:
                    sampled[group] = True
                    new_centers.append((group, l))
                elif len(l) > k:
                    sampled[group] = True
                    new_centers.append((group, random.sample(l, k)))

            print sampled_counts
            print sampled

            resample = False
            for group, group_sampled in enumerate(sampled):
                if not group_sampled:
                    resample = True
                    break

        return dict(new_centers)

    def _assign(self, centers, kv_pairs):
        # (group, value) -> ((group, center), value)
        return kv_pairs.map(lambda p: ((p[0], closest_center(centers[p[0]], p[1])), p[1]))

    def _compute_centers(self, assigned_values):
        # ((group, center), value) -> ((group, center), (value, 1))
        assigned_values_count = assigned_values.map(lambda p: (p[0], (p[1], 1)))

        # ((group, center), (value, 1)) -> ((group, center), (sum_vec, count))
        center_sums = assigned_values_count.reduceByKey(lambda left, right: (left[0] + right[0], left[1] + right[1]))

        # ((group, center), (sum_vec, count)) -> (group, new_center))
        new_centers = center_sums.map(lambda p: (p[0][0], p[1][0] / float(p[1][1])))

        new_centers = new_centers.collect()

        # dict[group] = [centers]
        group_dict = defaultdict(list)
        for g, vec in new_centers:
            group_dict[g].append(vec)

        return group_dict

    def _flatten(self, assigned_values, n_groups):
        # ((group, center), value) -> (group, value)
        kv_pairs = assigned_values.map(lambda p: (p[0][1] * n_groups + p[0][0], p[1]))
        return kv_pairs

    def train(self, data):
        # (group, value)
        gv_pairs = data.map(lambda p: (0, p))
        n_groups = 1
        for i in xrange(self.n_levels):
            print "Level", i
            # (group, vec) -> dict(group) = [center_vec]
            centers = self._init_random(gv_pairs, n_groups, 2)
            for j in xrange(self.n_iter):
                print "Level", i, "Iterations", j
                # dict(group)=[center_vec], (group, value) -> ((group, center), value)
                assigned_values = self._assign(centers, gv_pairs)
                # ((group, center), value) -> dict[group] = [center_vec]
                centers = self._compute_centers(assigned_values)
            # flatten
            # ((group, center), value) -> (group, value)
            n_groups *= 2
            grouped_values = self._flatten(self, assigned_values, n_groups)
        return centers

def write_points(flname, points):
    fl = open(flname, "w")
    for p in points:
        string = " ".join(map(str, p))
        fl.write(string)
        fl.write("\n")
    fl.close()

def run_hierarchical(data_points):
    levels = np.log2(N_CLUSTERS)
    centers = _naive_cluster(data_points, levels)

    write_points("hierarchical_centers.txt", centers)

    return centers

def _naive_cluster(data_points, level):
    model = KMeans.train(data_points, 
                         2,
                         maxIterations=ITER, runs=10,
                         initializationMode="k-means||")
    
    if level == 1:
        return model.centers
    
    partitioned_data = data_points.map(lambda p: (model.predict(p), p))

    new_centers = []    
    for i in xrange(2):
        partition = partitioned_data.filter(lambda t: t[0] == i).map(lambda t: t[1])
        new_centers.extend(_naive_cluster(partition, level - 1))
        
    return new_centers        

def run_kmeans(parsedData):
    clusters = KMeans.train(parsedData, N_CLUSTERS, maxIterations=ITER, 
                            runs=10, initializationMode="k-means||")

    write_points("kmeans_centers.txt", clusters.centers)

    return clusters

def main(argv):
    data_flname = argv[1]

    sc = SparkContext("local", "AMD GSS Test")

    data = sc.textFile(data_flname)
    
    parsedData = data.map(lambda line: array([float(x) for x in line.split(' ')]))

#    kmeans_clusters = run_kmeans(parsedData)

    hierarchical_clusters = run_hierarchical(parsedData)    


if __name__ == "__main__":
    main(sys.argv)
