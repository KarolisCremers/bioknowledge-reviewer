#!/bin/bash

python3 wd_to_neo4j.py -c config.cfg --mediawiki_api_url http://localhost:8181/w/api.php --sparql_endpoint_url http://localhost:8282/proxy/wdqs/bigdata/namespace/wdq/sparql --node-out-path ~/Krusty/test_node_output.csv --edge-out-path ~/Krusty/test_edge_output.csv
