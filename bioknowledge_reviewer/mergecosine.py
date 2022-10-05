#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep 15 20:30:07 2022

@author: karolis
"""

import pandas as pd


scores = pd.read_csv("cosine_included.csv")
cosine = pd.read_csv("cosine_scores.csv", index_col=0)
#cosine.columns = ["Node2.id", "cosinesim"]

merged = scores.join(cosine, on="Node2.id", how='left', lsuffix='_left', rsuffix='_right')
merged.to_csv("cosine_included.csv")
