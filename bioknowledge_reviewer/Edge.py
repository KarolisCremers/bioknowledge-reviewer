#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Nov 13 21:23:39 2020

@author: karolis
"""

class Edge():
    """
    This is a minimal Edge object. Currently not used in the modules.
    May be a starting point of OOP conversion of project.
    Current function is to rapidly convert edge properties
    into an dict object. 
    """
    def __init__(self, object_id,
                 subject_id="ensembl:ENSG00000197386",
                 property_id="RO:0002434", 
                 property_label="interacts with",
                 property_description="A relationship that holds between two entities in which the processes executed by the two entities are causally connected.",
                 property_uri="http://purl.obolibrary.org/obo/RO_0002434",
                 reference_uri="PMID:26636579",
                 reference_supporting_text='Here we present a genome-wide analysis of mRNA expression in human prefrontal cortex from 20 HD and 49 neuropathologically normal controls using next generation high-throughput sequencing.',
                 reference_date='2015-11-04'):
        self.object_id = object_id
        self.subject_id = subject_id
        self.property_id = property_id
        self.property_label = property_label
        self.property_description = property_description
        self.property_uri = property_uri
        self.reference_uri = reference_uri
        self.reference_supporting_text = reference_supporting_text
        self.reference_date = reference_date
    
    def get_dict(self):
        """
        This function returns the edge object attributes
        in a dictionary format.

        Returns
        -------
        dict
            Edge attributes given to the edge objects in a
            dictionary format.

        """
        return {'object_id': self.object_id,
                'subject_id': self.subject_id,
                'property_id': self.property_id,
                'property_label': self.property_label,
                'property_description': self.property_description,
                'property_uri': self.property_uri,
                'reference_uri': self.reference_uri,
                'reference_supporting_text': self.reference_supporting_text,
                'reference_date': self.reference_date}