#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 21 13:06:00 2022

@author: karolis
"""

import pandas as pd

nodes_file = pd.DataFrame.from_csv("graph_nodes_dummy.csv")
print(nodes_file.columns)
print(nodes_file)
nodes_file['synonyms:IGNORE'] = nodes_file[['synonyms:IGNORE', 'name']].astype(str).agg('|'.join, axis=1)
del nodes_file['name']
print(nodes_file.columns)
print(nodes_file)
nodes_file.to_csv("graph_nodes_dummy_fixed.csv")