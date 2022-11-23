#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov  8 19:09:37 2022

@author: karolis

n = 408083 edges within the graph (r)
r = 31539 nodes (n)

==== Class imbalance normalization ====
q = n * (n-1) / 2 = 497338491
(q - r) / r = 15756,061003837 True class ratio or negativeSamplingRatio
1 / 15756,061003837 = 0,000063468 negativeClassWeight
=======================================
"""

import sklearn.model_selection
import sklearn.ensemble as ensemble
import pandas as pd


# TODO simplify loading functions, not needed?
def LoadNodesCSV(path):
    """
    This function reads a node file

    Parameters
    ----------
    path : String
        Path to file.

    Returns
    -------
    nodes : Pandas DataFrame
        Table of nodes.

    """
    nodes = pd.read_csv(path, sep=",")
    return nodes


def LoadEmbeddings(path):
    """
    This function loads the FastRP embeddings from
    the FastRP.py script output file
    Embeddings are in the same order as the
    nodes csv file.

    Parameters
    ----------
    path : string
        Path to a node embedding file

    Returns
    -------
    embeddings : Pandas DataFrame
        Table where every row contains the embedding
        of a node corresponding to the node within the
        nodes CSV file. dimentions=(277804 rows, 512 col)

    """
    embeddings = pd.read_csv(path, sep=",", header=None)
    return embeddings


def LoadEdgesCSV(path):
    edges = pd.read_csv(path, sep=",")    
    return edges


def CreateNodeMap(nodes):
    """
    This function generates mappings
    between node indexes (used in NEO4J)
    and node names/id's.

    Parameters
    ----------
    nodes : Pandas DataFrame
        Table where every row contains the embedding
        of a node corresponding to the node within the
        nodes CSV file. dimentions=(277804 rows, 512 col) 

    Returns
    -------
    node_map : dictionary
        key = node id, value = index/NEO4J internal id
    index_map : dictionary
        key = index/NEO4J internal id, value = node id

    """
    node_map = {}
    index_map = {}
    for index, row in nodes.iterrows():
        node_id = row["id:ID"]
        node_map[node_id] = index
        index_map[index] = node_id
    return node_map, index_map


def CheckTracker(tracker, pair):
    """
    This function checks if the pair of
    nodes has already been found within the
    collection of edges. This is done 
    independent of edge type or edge
    direction. IE, using undericted edge
    principles.

    Parameters
    ----------
    tracker : set
        A hashed collection of node pairs
        with pattern: ((A,B), (B, A))
    pair : tuple
        The pair of nodes that have 
        an edge.

    Returns
    -------
    tracker : set
        A hashed collection of node pairs
        with pattern: ((A,B), (B, A))
    pair : tuple, None
        The pair of nodes that have an edge.
        returns None if pair has
        already been found within the tracker
        set.

    """
    start_size = len(tracker)
    reverse_pair = (pair[1], pair[0])
    tracker.add((pair, reverse_pair))
    if start_size < len(tracker): #If new unique pair is added to set
        return tracker, pair # return the pair
    else:
        return tracker, None # return a placeholder


def ExtractUndirected(edges, node_map):
    """
    This function transforms and extracts
    all edges in a undirected format.
    All edge type information is lost, duplicate
    edges are removed.

    Parameters
    ----------
    edges : Pandas DataFrame
        Table of edges
    node_map : Dictionary
        Mapping of node id to index. (see CreateNodeMap)

    Returns
    -------
    unique_edges : Pandas DataFrame
        A list of node pairs with an generic
        undirected edge (implied, no edge type given)
    unique_edges_ids : Pandas DataFrame
        A list of node pairs with an generic
        undirected edge (implied, no edge type given)
        Node indexes are given as representations.

    """
    # get unique sets of pairwise nodes.
    tracker = set()
    unique_edges = set()
    unique_edges_ids = list()
    for index, row in edges.iterrows():
        pair = (row[":START_ID"], row[":END_ID"])
        tracker, filtered_pair = CheckTracker(tracker, pair)
        unique_edges.add(filtered_pair)
        if filtered_pair:
            unique_edges_ids.append((node_map[pair[0]], node_map[pair[1]]))
    unique_edges.remove(None)
    unique_edges = pd.DataFrame(unique_edges, columns=[":START_ID",":END_ID"])
    unique_edges_ids = pd.DataFrame(unique_edges_ids, columns=[":START_ID",":END_ID"])
    return unique_edges, unique_edges_ids


def AddNegativeExample(nodes, edges, distribution_ratio,
                       negative_class_weight):
    print("TODO!")


def AddNegativeExampleEmbeddings(embeddings, edges, distribution_ratio,
                                 negative_class_weight):
    #TODO make this a generator that spits out
    # negative examples based on the distribution ratio
    # will mean that training function will be run
    # on a sample per sample or batch wise basis.
    edges_bool = list()
    weights = list()
    dataset = pd.DataFrame()
    tracker = set()
    target_size = len(edges) * distribution_ratio
    print("Current size = {}".format(len(edges)))
    print("target size of edges = {}".format(target_size))
    
    return dataset, edges_bool, weights


def CalculateClassWeights(nodes, edges):
    """
    Parameters
    ----------
    nodes : Pandas DataFrame
        The list of nodes and node attributes
        obtained through LoadNodes.
    edges : Pandas DataFrame
        The list of edges that exists within
        the graph.

    Returns
    -------
    negative_sampling_ratio : integer
        The amount of negative samples needed to replicate
        current non-edge frequency for the dataset.
    negative_class_weight : float
        The training weight that is assigned to a single
        negative example compared to a positive example
        during model training.
    """
    # add references
    total_nodes = len(nodes)
    total_edges = len(edges)
    # TODO
    # total_edges should be undirected edges, 
    # right now we have duplicate "undirected" edges between nodes.
    max_undirected_edges = total_nodes * (total_nodes - 1) / 2
    negative_sampling_ratio = (max_undirected_edges - total_edges) / total_edges
    negative_class_weight = 1 / negative_sampling_ratio
    return negative_sampling_ratio, negative_class_weight


def DistributeDataset(samples, edges_bool, ratio=(6,2,2)):
    test_ratio = ratio[1] / sum(ratio)
    node_pair_train, node_pair_test, has_edge_train,\
        has_edge_test = sklearn.model_selection.train_test_split(samples,\
        edges_bool, test_size=test_ratio, random_state=1)
    val_ratio = ratio[2] / (sum(ratio) - ratio[1])
    node_pair_train, node_pair_val, has_edge_train,\
        has_edge_val = sklearn.model_selection.train_test_split(node_pair_train, has_edge_train,\
                                        test_size=val_ratio, random_state=1) # 0.25 x 0.8 = 0.2
    return [node_pair_train, has_edge_train, node_pair_test, has_edge_test,\
            node_pair_val, has_edge_val]


def TrainModelEmbedding(node_pair_train, has_edge_train, sample_weight):
    model = ensemble.HistGradientBoostingClassifier(random_state=1, verbose=1)
    model.fit(node_pair_train, has_edge_train, sample_weight=sample_weight)
    return


def TrainModelAttributes(node_pair_train, has_edge_train, sample_weight):
    model = ensemble.HistGradientBoostingClassifier(catagorical_features=[], random_state=1, verbose=1)
    model.fit(node_pair_train, has_edge_train, sample_weight=sample_weight)
    return

def main():
    print("loading edge csv")
    edges = LoadEdgesCSV("/home/karolis/LUMC/HDSR/bioknowledge-reviewer/bioknowledge_reviewer/neo4j-community-4.2.1/import/HD/v2022-10-26/HD_statements.csv")
    print("loading node csv")
    nodes = LoadNodesCSV("/home/karolis/LUMC/HDSR/bioknowledge-reviewer/bioknowledge_reviewer/neo4j-community-4.2.1/import/HD/v2022-10-26/HD_concepts.csv")
    print("Creating node mappings")
    node_map, index_map = CreateNodeMap(nodes)
    print("Extracting unique edges")
    unique_edges, unique_edges_ids = ExtractUndirected(edges, node_map)
    print("Calculating negative sampling ratio and class weights")
    sampling_ratio, negative_class_weight = CalculateClassWeights(nodes, edges)
    print("Adding negative samples based on ratio")
    dataset, has_real_edge = AddNegativeExampleEmbeddings(nodes, edges,
                                                          sampling_ratio,
                                                          negative_class_weight)
    # print("Distributing dataset")
    # node_pair_train, has_edge_train, node_pair_test, has_edge_test,\
    #         node_pair_val, has_edge_val = DistributeDataset(dataset, has_real_edge)
    # print("Training model")
    
    
if __name__ == "__main__":
    main()
    
    