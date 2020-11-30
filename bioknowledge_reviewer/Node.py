#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Nov 13 20:55:39 2020

@author: karolis
"""

class Node():
    
    def __init__(self, ID, semantic_groups="NA",
                 preflabel="NA", name="NA", synonyms="NA", description="NA"):
        self.id = ID
        self.semantic_groups = semantic_groups
        self.preflabel = preflabel
        self.name = name
        self.synonyms = synonyms
        self.description = description
    
    def get_dict(self):
        return {"id": self.id, "semantic_groups": self.semantic_groups,
                "preflabel": self.preflabel, "name": self.name,
                "synonyms": self.synonyms, "description": self.description}
    
    