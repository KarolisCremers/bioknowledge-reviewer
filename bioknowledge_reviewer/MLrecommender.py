#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep  7 10:20:08 2022

@author: karolis
"""


# ML recommender
from neo4j import GraphDatabase
import sys,os
import json
import yaml
import datetime
import neo4j.exceptions
import pandas as pd
from tqdm import tqdm
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

port = "7687"

# Fast Random Projection:https://neo4j.com/docs/graph-data-science/1.8/algorithms/fastrp/
# Graph node size: 408.288, 100.000 = 256 embedding dimension size
# Ebedding size = 512

# Normalization strength negative values downplays the importance of
# high degree nodes. The algorithmic predictions all rely  
# at least partially on node degrees. we can test both but I 
# suspect link prediction for low degree nodes is more valueable

# iteration weight determines the degree of neigbours embedded into
# the vectors. To keep the embeddings relatively local, we'll try
# to limit to the 3rd degree neigbours. With equal value for all
# iterations

# Node self influence value causes the node itself to be included
# into the projection vector. This is needed to ensure non-zero 
# vectors for low-degree nodes. If positive, the nodes own
# properties are can be taken into account.

# orientation variable describes which type of directional
# edges should be used to make the projection.
# default uses all. And this seems a good start
today = datetime.date.today()
frp_config = {"propertyRatio": 1.0,
              "featureProperties": ["Type"],
              "embeddingDimension": 512,
              "iterationWeights": [1.0,1.0,1.0],
              "normalizationStrength": -1.0,
              "randomSeed": 7687}

direct = os.getcwd() + "/embeddings"
if not os.path.isdir(direct): os.makedirs(direct)

def fastrp(config):
    """
    This function calls the Fast Random Projection
    function on the neo4j server.
    
    returns:
        projections
    
    """
    try:
        driver = GraphDatabase.driver("bolt://localhost:{}".format(port),
                                      auth=("neo4j", "HD"))
    except neo4j.exceptions.ServiceUnavailable:
        raise
    print("\n Projecting nodes \n")
    with driver.session() as session:
        query = """ CALL gds.fastRP.stream({nodeQuery: 'MATCH (n)-[]-(m) return distinct id(m) as id',relationshipQuery:'MATCH (n)-[e]-(m) WHERE not type(e) = "RO:HOM0000020" AND not type(e) = "RO:HOM0000017" return id(n) as source, id(m) as target', propertyRatio: 0.0, featureProperties: [], embeddingDimension: 512,iterationWeights: [1.0,1.0,1.0],normalizationStrength: -1.0, randomSeed:7687,validateRelationships:false})
YIELD nodeId, embedding"""
        result = session.run(query)
        f = open(direct + "/embeddings_{}".format(today.isoformat()), "w")
        f.close()
        with open(direct + "//embeddings_{}".format(today.isoformat()), "a") as file:
            for ix, record in enumerate(result):
                file.write(str(record.values()[0]) + "," + 
                           str(record.values()[1]), end="\n")
    return "done"

# output = fastrp(frp_config)
# print(output)


def fastrp_targets(config, targets):
    """
    This function calls the Fast Random Projection
    function on the neo4j server.
    
    returns:
        projections
    
    """
    try:
        driver = GraphDatabase.driver("bolt://localhost:{}".format(port),
                                      auth=("neo4j", "HD"))
    except neo4j.exceptions.ServiceUnavailable:
        raise
    print("\n targets \n")
    with driver.session() as session:
        query = """ CALL gds.fastRP.stream({nodeQuery: 'MATCH (n)-[]-(m) return distinct id(m) as id',relationshipQuery:'MATCH (n)-[e]-(m) WHERE not type(e) = "RO:HOM0000020" AND not type(e) = "RO:HOM0000017" return id(n) as source, id(m) as target', propertyRatio: 0.0, featureProperties: [], embeddingDimension: 512,iterationWeights: [1.0,1.0,1.0],normalizationStrength: -1.0, randomSeed:7687,validateRelationships:false})
YIELD nodeId, embedding"""
        result = session.run(query)
        results = []
        for ix, record in enumerate(result):
            if ix in targets:
                results.append((record.values()[0], record.values()[1]))
                #TODO add the associated target gene to the record list
    return results


def query_id(node1, node2):
    try:
        driver = GraphDatabase.driver("bolt://localhost:{}".format(port),
                                      auth=("neo4j", "HD"))
    except neo4j.exceptions.ServiceUnavailable:
        raise
    with driver.session() as session:
        # WHERE filter removes hommologous edges.
        query = """MATCH (Node1 {id:"%s"})-[]-()-[]-(Node2 {id:"%s"}) RETURN id(Node1), id(Node2)"""%(node1,node2)
        result = session.run(query)
        result_list = result.values()
        
    return result_list


path = "/home/karolis/LUMC/HDSR/bioknowledge-reviewer/bioknowledge_reviewer/recommended_edges.csv"

def cosine_sim(path):
    recommendations = pd.read_csv(path)
    pairs = recommendations.loc[:,["Node1.id", "Node2.id"]]
    targets = [0]
    print("pairs")
    xref = {0:"HGNC:4851"}
    for index, row in pairs.iterrows():
        result_list = query_id(row[0], row[1])
        node1, node2 = result_list[0]
        targets.append(node2)
        xref[node2] = row[1]
    vectors = fastrp_targets(frp_config, targets)
    HTT = vectors[0]
    sim_score = pd.DataFrame()
    print("Cosine")
    for vector in vectors:
        similarity = cosine_similarity(np.array(HTT[1]).reshape(1,-1),np.array(vector[1]).reshape(1,-1))
        sim_score.loc[xref[vector[0]],"cosinesim"] = similarity[0][0]
    return sim_score

scores = cosine_sim(path)
print(scores)
scores.to_csv("cosine_scores.csv")