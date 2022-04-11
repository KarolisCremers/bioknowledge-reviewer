#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar  3 14:56:02 2022

@author: karolis
"""


from wikibaseintegrator.wbi_config import config as wbi_config
from wikibaseintegrator import wbi_core, wbi_login

wbi_config['MEDIAWIKI_API_URL'] = 'http://localhost:80/api.php'
wbi_config['SPARQL_ENDPOINT_URL'] = 'http://localhost:8834/proxy/wdqs/bigdata/namespace/wdq/sparql'
wbi_config['WIKIBASE_URL'] = 'http://localhost:80'
wbi_config['BACKOFF_MAX_TRIES'] = 1
# login object

# We have raw data, which should be written to Wikidata, namely two human NCBI entrez gene IDs mapped to two Ensembl Gene IDs
# = {
#    '50943': 'ENST00000376197',
#    '1029': 'ENST00000498124'
#}

print("login")
login_instance = wbi_login.Login(user='Kmpcremers@testbot', pwd='d7ahbf33c1jts9nesc49fltgfc1lv1bb')
# data type object, e.g. for a NCBI gene entrez ID
print("create string")
entrez_gene_id = wbi_core.String(value='1029', prop_nr='p3')

# data goes into a list, because many data objects can be provided to
data = [entrez_gene_id]

# Search for and then edit/create new item
print("Engine")
wd_item = wbi_core.ItemEngine(data=data,item_id='Q0', new_item=True)
print("write")
wd_item.write(login_instance)
