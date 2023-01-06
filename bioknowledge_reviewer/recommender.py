#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Aug 13 11:28:59 2022

@author: karolis

Edge prediction module
"""

from neo4j import GraphDatabase
import sys,os
import json
import yaml
import datetime
import neo4j.exceptions
import pandas as pd
from tqdm import tqdm



today = datetime.date.today()
port = "7687"
topologicalAlg = {"adamicadar": "adamicAdar", 
 "Cneighours": "commonNeighbors",
 "prefAttach": "preferentialAttachment",
 "resAllocat": "resourceAllocation",
# "sameCommun": "sameCommunity",
 "totalNeigh": "totalNeighbors"}
# same community is not a reasonable algorithm to use, except if
# we define a community value, based on network modules or something

# test query on 25 limit

print("started")
def test_query(algorithm="adamicAdar", limit=25):
    try:
        driver = GraphDatabase.driver("bolt://localhost:{}".format(port), auth=("neo4j", "HD"))
    except neo4j.exceptions.ServiceUnavailable:
        raise
    with driver.session() as session:
        query = """ MATCH (p1)-[]-()-[]-(p2) RETURN p1.id, p2.id, gds.alpha.linkprediction.{}(p1, p2) AS {}score LIMIT {};""".format(algorithm, algorithm, limit)
        result = session.run(query)
        result_list = result.values()
        df = pd.DataFrame(result_list, columns=["Concept 1", "Concept 2",
                                                algorithm + "score"])
        print(df)
        
# for shorthand in topologicalAlg:
#     test_query(topologicalAlg[shorthand])
#test_query(topologicalAlg["resAllocat"], limit=200)

# 279 395 988 total pair-comparisons to be done. just calling them takes 2,3551 minutes
# focus on predicting edges originating from HTT?
text = """MATCH (Node1:GENE {id:"HGNC:4851"})-[]->()-[]->(Node2) RETURN Node1.id, Node2.id, gds.alpha.linkprediction.adamicAdar(Node1, Node2) AS adamicAdar,
        gds.alpha.linkprediction.commonNeighbors(Node1, Node2) AS commonNeighbors,
        gds.alpha.linkprediction.preferentialAttachment(Node1, Node2) AS preferentialAttachment,
        gds.alpha.linkprediction.resourceAllocation(Node1, Node2) AS resourceAllocation,
        gds.alpha.linkprediction.totalNeighbors(Node1, Node2) AS totalNeighbors;"""

def multi_query(limit=25):
    try:
        driver = GraphDatabase.driver("bolt://localhost:{}".format(port), auth=("neo4j", "HD"))
    except neo4j.exceptions.ServiceUnavailable:
        raise
    with driver.session() as session:
        query = """ MATCH (Node1)-[]-()-[]-(Node2)
        RETURN Node1.id, Node2.id, gds.alpha.linkprediction.adamicAdar(Node1, Node2) AS adamicAdar,
        gds.alpha.linkprediction.commonNeighbors(Node1, Node2) AS commonNeighbors,
        gds.alpha.linkprediction.preferentialAttachment(Node1, Node2) AS preferentialAttachment,
        gds.alpha.linkprediction.resourceAllocation(Node1, Node2) AS resourceAllocation,
        gds.alpha.linkprediction.totalNeighbors(Node1, Node2) AS totalNeighbors;"""
        result = session.run(query)
        result_list = result.values()
        df = pd.DataFrame(result_list, columns=result.keys())
        # sort by the least populated areas of the graph to maximise relative information gain.
        print(df.sort_values(by="totalNeighbors"))
        return df.sort_values(by="totalNeighbors")

#df = multi_query()

def get_targets():
    try:
        driver = GraphDatabase.driver("bolt://localhost:{}".format(port), auth=("neo4j", "HD"))
    except neo4j.exceptions.ServiceUnavailable:
        raise
    print("get targets")
    with driver.session() as session:
        # WHERE filter removes hommologous edges.
        query = """ MATCH (Node1 {id:"HGNC:4851"})-[]-()-[e]-(Node2)
        WHERE not type(e) = 'RO:HOM0000020' AND not type(e) = 'RO:HOM0000017' AND not (Node1)-[]-(Node2)
        RETURN Node1.id, collect(distinct Node2.id);"""
        result = session.run(query)
        result_list = result.values()
        df = pd.DataFrame(result_list, columns=result.keys())
        # sort by the least populated areas of the graph to maximise relative information gain.
        #print(df.sort_values(by="totalNeighbors"))
        return df

targets = get_targets()


def filter_paths(targets):
    """
    This function filters the target nodes based on the paths
    between the two nodes. For example, if a gene already interacts
    with the human variant of a target orthologous gene, it does
    not make sense to expect a new edge to between the gene and 
    the target in the context of a human centric hypothesis.
    """
    filtered_targets = targets
    return filtered_targets

filtered_targets = filter_paths(targets)


def query_targets(df):
    try:
        driver = GraphDatabase.driver("bolt://localhost:{}".format(port),
                                      auth=("neo4j", "ngly1"))
    except neo4j.exceptions.ServiceUnavailable:
        raise
    result_pd = pd.DataFrame(index=["Node1.id", "Node2.id", "adamicAdar",
                                    "commonNeighbors", 
                                    "preferentialAttachment", 
                                    "resourceAllocation", "totalNeighbors"])
    print("\n calculate scores")
    for target in tqdm(df.iloc[0][1]):
        with driver.session() as session:
            query = """ MATCH (Node1:GENE {id:"HGNC:4851"}) MATCH (Node2 {id:"%s"}) RETURN Node1.id, Node2.id, gds.alpha.linkprediction.adamicAdar(Node1, Node2) AS adamicAdar,
            gds.alpha.linkprediction.commonNeighbors(Node1, Node2) AS commonNeighbors,
            gds.alpha.linkprediction.preferentialAttachment(Node1, Node2) AS preferentialAttachment,
            gds.alpha.linkprediction.resourceAllocation(Node1, Node2) AS resourceAllocation,
            gds.alpha.linkprediction.totalNeighbors(Node1, Node2) AS totalNeighbors;"""%(target)
            result = session.run(query)
            result_list = result.values()[0]
            result_pd = pd.concat([ pd.Series(result_list, 
                                              index=["Node1.id", "Node2.id",
                                                     "adamicAdar", 
                                                     "commonNeighbors",
                                                     "preferentialAttachment",
                                                     "resourceAllocation",
                                                     "totalNeighbors"]),
                                   result_pd], axis=1)
    return result_pd.T
        
results = query_targets(filtered_targets)
print(results)
results.to_csv("query_targets_onto.csv")

def recommend(results, inter=float(1)):
    """
    This function prioritises edge predictions that
    are fast to interpret with a high likelyhood of 
    being correct.
    This means that it balances confidence and
    number of neigbours.
    inter:
        How much do understandable results weigh in
        the recommendations
        float, between 0 and 1.
    """
    print("reccomender")
    if inter == 0:
        inter = 0.0001
    weight = 1/inter # adamicadar OR resource allocation score / (number of common neigbours * weight)
    
    results["weightedadamicAdar"] = results["adamicAdar"] / (
        (results["commonNeighbors"] + 1) * weight)
    results["weightedadamicAdar"] = results[
        "weightedadamicAdar"] / results["weightedadamicAdar"].max()
    
    results["weightedresourceAllocation"] = results["resourceAllocation"] / (
        (results["commonNeighbors"] + 1) * weight)
    results["weightedresourceAllocation"] = results[
        "weightedresourceAllocation"] / results[
            "weightedresourceAllocation"].max()
            
    results["weightedpreferentialAttachment"] = results["preferentialAttachment"] / (
        (results["commonNeighbors"] + 1) * weight)
    results["weightedpreferentialAttachment"] = results[
        "weightedpreferentialAttachment"] / results[
            "weightedpreferentialAttachment"].max()
            
    # sort results by the three new weighted scores
    results["recommendationscore"] = results["weightedadamicAdar"] + results[
        "weightedresourceAllocation"] + results[
            "weightedpreferentialAttachment"]
    results["recommendationscore"] = results["recommendationscore"] / 3
    return results

Recommended = recommend(results)
print(Recommended)
Recommended.to_csv("recommended_edges_onto.csv")
