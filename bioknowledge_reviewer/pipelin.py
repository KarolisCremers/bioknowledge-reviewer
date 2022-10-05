#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 28 12:12:02 2022

@author: karolis
"""
from neo4j import GraphDatabase
import neo4j.exceptions
import pandas as pd
from tqdm import tqdm
import sys,os
import datetime

port = "7687"
today = datetime.date.today()

direct = os.getcwd() + "/clusters"
if not os.path.isdir(direct): os.makedirs(direct)

graph_name = "HD"
pipeline_name = "recommender"

def graph_projection(name):
    try:
        driver = GraphDatabase.driver("bolt://localhost:{}".format(port),
                                      auth=("neo4j", "HD"))
    except neo4j.exceptions.ServiceUnavailable:
        raise
    print("\n Projecting nodes \n")
    with driver.session() as session:
        query = """ CALL gds.graph.create.cypher('%s', "MATCH (m) return id(m) as id, LABELS(m) as TYPE", "MATCH (n)-[e]-(m) return id(n) as source, id(m) as target, type(e) as type")"""%(name)
        result = session.run(query)
    return "graph projection done"

print(graph_projection(graph_name))

def clustering(name):
    try:
        driver = GraphDatabase.driver("bolt://localhost:{}".format(port),
                                          auth=("neo4j", "HD"))
    except neo4j.exceptions.ServiceUnavailable:
        raise
    print("\n clustering \n")
    with driver.session() as session:
        query = """ CALL gds.louvain.stream('%s')"""%(name)
        result = session.run(query)
        f = open(direct + "/clusters_{}".format(today.isoformat()), "w")
        f.close()
        with open(direct + "/clusters_{}".format(today.isoformat()), "a") as file:
            for ix, record in enumerate(result):
                file.write(str(record.values()).strip("[]") + "\n")
    return "Clustering done"

print(clustering(graph_name))


def fastrp(name):
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
        query = """ CALL gds.fastRP.stream('%s', {nodeQuery: 'MATCH (n)-[]-(m) return distinct id(m) as id',relationshipQuery:'MATCH (n)-[e]-(m) WHERE not type(e) = "RO:HOM0000020" AND not type(e) = "RO:HOM0000017" return id(n) as source, id(m) as target', propertyRatio: 0.0, featureProperties: [], embeddingDimension: 512,iterationWeights: [1.0,1.0,1.0],normalizationStrength: -1.0, randomSeed:7687,validateRelationships:false})
YIELD nodeId, embedding"""%(name)
        result = session.run(query)
        f = open(direct + "/embeddings_{}".format(today.isoformat()), "w")
        f.close()
        with open(direct + "/embeddings_{}".format(today.isoformat()), "a") as file:
            for ix, record in enumerate(result):
                # important! the embeddings are ordered from nodeid 0 to max in the file, each line is one node!
                file.write(str(record.values()[1]).strip("[]") + "\n")
    return "Node projection done"

#print(fastrp(graph_name))


def generate_pipeline(pipeline_name):
    try:
        driver = GraphDatabase.driver("bolt://localhost:{}".format(port),
                                          auth=("neo4j", "HD"))
    except neo4j.exceptions.ServiceUnavailable:
        raise
    print("\n generate link prediction pipeline \n")
    with driver.session() as session:
        query = """ CALL gds.alpha.ml.pipeline.linkPrediction.create('%s')"""%(pipeline_name)
        result = session.run(query)
    return "generated basic pipeline"

print(generate_pipeline(pipeline_name))

def include_node_property(pipeline_name):
    try:
        driver = GraphDatabase.driver("bolt://localhost:{}".format(port),
                                          auth=("neo4j", "HD"))
    except neo4j.exceptions.ServiceUnavailable:
        raise
    print("Add node embedding \n")
    with driver.session() as session:
        query = """ CALL gds.alpha.ml.pipeline.linkPrediction.addNodeProperty('%s', 'fastRP',{
  mutateProperty: 'embedding',
  propertyRatio: 0.0, featureProperties: [], embeddingDimension: 512,iterationWeights: [1.0,1.0,1.0],normalizationStrength: -1.0, randomSeed:7687})"""%(pipeline_name)
        result = session.run(query)
    return "Added node embeddings"

print(include_node_property(pipeline_name))


def add_features(pipeline_name):
    try:
        driver = GraphDatabase.driver("bolt://localhost:{}".format(port),
                                          auth=("neo4j", "HD"))
    except neo4j.exceptions.ServiceUnavailable:
        raise
    print("set node features\n")
    with driver.session() as session:
        query = """ CALL gds.alpha.ml.pipeline.linkPrediction.addFeature('%s', 'hadamard', {nodeProperties: ['embedding']})"""%(pipeline_name)
        result = session.run(query)
    return "finished setting node features"    

print(add_features(pipeline_name))


def set_split(pipeline_name):
    try:
        driver = GraphDatabase.driver("bolt://localhost:{}".format(port),
                                          auth=("neo4j", "HD"))
    except neo4j.exceptions.ServiceUnavailable:
        raise
    print("set train split\n")
    with driver.session() as session:
        query = """ CALL gds.alpha.ml.pipeline.linkPrediction.configureSplit('%s',{})"""%(pipeline_name)
        result = session.run(query)
    return "finished setting up train split"

print(set_split(pipeline_name))

def set_model(pipeline_name):
    try:
        driver = GraphDatabase.driver("bolt://localhost:{}".format(port),
                                          auth=("neo4j", "HD"))
    except neo4j.exceptions.ServiceUnavailable:
        raise
    print("set model\n")
    with driver.session() as session:
        query = """ CALL gds.alpha.ml.pipeline.linkPrediction.configureParams('%s',[])"""%(pipeline_name)
        result = session.run(query)
    return "finished setting up model"    

print(set_model(pipeline_name))

def train_model(pipeline_name):
    try:
        driver = GraphDatabase.driver("bolt://localhost:{}".format(port),
                                          auth=("neo4j", "HD"))
    except neo4j.exceptions.ServiceUnavailable:
        raise
    print("train model\n")
    with driver.session() as session:
        query = """ CALL gds.alpha.ml.pipeline.linkPrediction.train('%s', {modelName: '%s', pipeline: '%s', randomSeed: 7687}) YIELD modelInfo"""%(graph_name, "testmodel", pipeline_name)
        result = session.run(query)
        print(result.consume())
    return "finished training model"    

print(train_model(pipeline_name))