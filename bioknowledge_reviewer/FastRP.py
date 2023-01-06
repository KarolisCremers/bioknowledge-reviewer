#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 16 09:56:18 2022

@author: karolis
"""
from neo4j import GraphDatabase
import sys,os
import json
import yaml
import datetime
import neo4j.exceptions
import pandas as pd
from tqdm import tqdm

frp_config = {"propertyRatio": 1.0,
              "featureProperties": ["Type"],
              "embeddingDimension": 512,
              "iterationWeights": [1.0,1.0,1.0],
              "normalizationStrength": -1.0,
              "randomSeed": 7687}

today = datetime.date.today()
port = "7687"
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
        query = """ CALL gds.fastRP.stream({nodeQuery: 'MATCH (n)-[]-(m) return distinct id(m) as id',relationshipQuery:'MATCH (n)-[e]-(m) return id(n) as source, id(m) as target', propertyRatio: 0.0, featureProperties: [], embeddingDimension: 512,iterationWeights: [1.0,1.0,1.0], normalizationStrength: -1.0, randomSeed:7687,validateRelationships:false})
YIELD nodeId, embedding"""
        result = session.run(query)
        f = open(direct + "/embeddings_{}".format(today.isoformat()), "w")
        f.close()
        with open(direct + "/embeddings_{}".format(today.isoformat()), "a") as file:
            for ix, record in enumerate(result):
                file.write(str(record.values()[1]).strip("[]") + "\n")
    return "done"

fastrp(frp_config)