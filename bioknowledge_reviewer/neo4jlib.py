# @name: neo4jlib.py
# @description: Module for Neo4j data structure and management
# @version: 1.0
# @date: 22-02-2018
# @author: Núria Queralt Rosinach
# @email: nuriaqr@scripps.edu

# All files before concatenation should have the same format:
# Statements format:
# :START_ID,
# :TYPE,
# :END_ID,
# reference_uri,
# reference_supporting_text,
# reference_date,
# property_label,
# property_description,
# property_uri

# Concepts format:
# id:ID,
# :LABEL,
# preflabel,
# synonyms:IGNORE,
# description


#TODO: update import function for Neo4j v3.5
"""Module for Neo4j"""

import datetime
import os, sys
import subprocess
import re
import time
from utils import *

# VARIABLES
today = datetime.date.today()

# NETWORK MANAGEMENT FUNCTIONS


def save_neo4j_files(object, neo4j_path, file_type = 'statements'):
    """
    This function saves the neo4j graph files in CSV format into the neo4j import directory.
    :param object: graph nodes or edges dataframe
    :param neo4j_path: path to neo4j directory string
    :param file_type: statements (default value) or concepts string
    :return: None object
    """

    # path to neo4j/
    # path = os.getcwd() + "/neo4j"
    # if not os.path.isdir(path): os.makedirs(path)

    # path_to_import
    graph_version = 'v{}'.format(today)
    path_to_import = neo4j_path + '/import/HD'
    path_to_version = neo4j_path + '/import/HD/' + graph_version
    for dir in path_to_import, path_to_version:
        if not os.path.isdir(dir): os.makedirs(dir)
    # save filling null and with sep=','
    if file_type == 'statements':
        object.to_csv('{}/HD_statements.csv'.format(path_to_import),
                          index=False, na_rep='NA')
        object.to_csv('{}/HD_statements.csv'.format(path_to_version),
                          index=False, na_rep='NA')
        # object.fillna('NA').to_csv('{}/HD_statements_v{}.csv'.format(path,today), index=False)
        return print("\nFile '{}/HD_statements.csv' saved.".format(path_to_import))
    elif file_type == 'concepts':
        object.to_csv('{}/HD_concepts.csv'.format(path_to_import),
                        index=False, na_rep='NA')
        object.to_csv('{}/HD_concepts.csv'.format(path_to_version),
                    index=False, na_rep='NA')
        # object.fillna('NA').to_csv('{}/HD_concepts_v{}.csv'.format(path, today), index=False)
        return print("\nFile '{}/HD_concepts.csv' saved.".format(path_to_import))
    else:
        return print('The user should provide the "file_type" argument with any of the [statements or concepts] value.')


# CHECK GRAPH SCHEMA AND NORMALIZE TO NEO4J FORMAT

def get_statements(edges):
    """
    This function returns the statements neo4j file format.
    :param edges: edges dataframe
    :return: neo4j statements dataframe
    """

    # check header
    statements = check_format(edges)
    # format header
    statements = (
        statements
            .rename(columns={
            'subject_id': ':START_ID',
            'property_id': ':TYPE',
            'object_id': ':END_ID',
            'property_description': 'property_description:IGNORE'
        }
        )
    )

    return statements


def get_concepts(nodes):
    """
    This function returns the concepts neo4j file format.
    :param nodes: nodes dataframe
    :return: neo4j concepts dataframe
    """

    # check header
    concepts = check_format(nodes, file_type='concepts')
    # format header
    concepts = (
        concepts
            .rename(columns={
            'id': 'id:ID',
            'semantic_groups': ':LABEL',
            'synonyms': 'synonyms:IGNORE'
        }
        )
    )

    return concepts


# CREATE Neo4j Community Server
def create_neo4j_instance(version='5.1.0'):
    """
    This function downloads an creates a Neo4j Community v3.5 server instance.
    :param version: Neo4j server version number string (default '4.2.1')
    :return: Neo4j server directory name string
    """

    print('Creating a Neo4j community v{} server instance...'.format(version))
    # download neo4j community server v3.5.X
    directory = 'neo4j-community-{}'.format(version)
    if not os.path.isfile('neo4j-community-{}-unix.tar.gz'.format(version)):
        print('Downloading the server from neo4j.org...')
        cmd = 'wget http://dist.neo4j.org/neo4j-community-{}-unix.tar.gz'.format(version)
        subprocess.call(cmd, shell=True)

    # untar server directory
    if not os.path.isdir('./neo4j-community-{}'.format(version)):
        print('Preparing the server...')
        cmd = 'tar -xf neo4j-community-{}-unix.tar.gz'.format(version)
        subprocess.call(cmd, shell=True)

    # update configuration file
    if os.path.isdir('./neo4j-community-{}'.format(version)):
        conf_filepath = os.path.join('.', directory, 'conf', 'neo4j.conf')
        with open(conf_filepath) as f:
            text = f.read()
        f.close()
        find = '#dbms.security.auth_enabled=false'
        pattern = re.escape(find)
        replace = 'dbms.security.auth_enabled=false'
        text = re.sub(pattern, replace, text)
        find = '#dbms.connectors.default_listen_address=0.0.0.0'
        pattern = re.escape(find)
        replace = 'dbms.connectors.default_listen_address=0.0.0.0'
        text = re.sub(pattern, replace, text)
        find = '#dbms.connector.bolt.listen_address=:7687'
        pattern = re.escape(find)
        replace = 'dbms.connector.bolt.listen_address=:7687'
        text = re.sub(pattern, replace, text)
        find = '#dbms.connector.http.listen_address=:7474'
        pattern = re.escape(find)
        replace = 'dbms.connector.http.listen_address=:7474'
        text = re.sub(pattern, replace, text)
        # Whitelist Graph data science plugin
        find = '#dbms.security.procedures.unrestricted=my.extensions.example,my.procedures.*'
        pattern = re.escape(find)
        replace = 'dbms.security.procedures.unrestricted=gds.*,n10.*,apoc.*'
        text = re.sub(pattern, replace, text)
        #find = '#dbms.security.procedures.allowlist=apoc.coll.*,apoc.load.*,gds.*'
        #pattern = re.escape(find)
        #replace = 'dbms.security.procedures.allowlist=apoc.coll.*,apoc.load.*,gds.*'
        #text = re.sub(pattern, replace, text)
        # add neo4j manual url as starting command on connection and neosemantics endpoint
        text += '\nbrowser.post_connect_cmd=play http://localhost:8001/html/guide.html\ndbms.unmanaged_extension_classes=n10s.endpoint=/rdf'
        with open(conf_filepath, 'wt') as f:
            f.write(text)
        f.close()
        print('Configuration adjusted!')
    
    # Graph data science plugin install
    if os.path.isdir('./neo4j-community-{}'.format(version)):
        print('Installing plugins...')
        plugin_filepath = os.path.join('.', directory, 'plugins')
        cmd = 'wget -P {} https://graphdatascience.ninja/neo4j-graph-data-science-2.3.2.zip'.format(plugin_filepath)
        subprocess.call(cmd, shell=True)
        cmd = 'unzip {}/neo4j-graph-data-science-2.3.2.zip -d {}'.format(plugin_filepath, plugin_filepath)
        subprocess.call(cmd, shell=True)
        cmd = 'wget -P {} https://github.com/neo4j-labs/neosemantics/releases/download/5.1.0.0/neosemantics-5.1.0.0.jar'.format(plugin_filepath)
        subprocess.call(cmd, shell=True)
        cmd = 'mv {}/labs/apoc-5.1.0-core.jar {}/apoc-5.1.0-core.jar'.format(directory, plugin_filepath)
        subprocess.call(cmd, shell=True)
	with open(os.path.join('.', directory, 'conf', 'apoc.conf')) as file:
		file.write("apoc.export.file.enabled=true")


    # start server and check is running (return answer)
    if not os.path.isfile('{}/run/neo4j.pid'.format(directory)):
        print('Starting the server...')
        cmd = './{}/bin/neo4j restart'.format(directory)
        subprocess.call(cmd, shell=True)

    # wait for 5 seconds and check
    time.sleep(30)
    if os.path.isfile('{}/run/neo4j.pid'.format(directory)):
        print('Neo4j v{} is running.'.format(version))
    else:
        print('Neo4j v{} is NOT running. Some problem occurred and should be checked. Bye!'.format(version))

    return directory


# LOAD GRAPH

def do_import(neo4j_path):
    """
    This function executes the import of the graph into the neo4j server v3.5 instance.
    :param neo4j_path: path to neo4j directory string
    :return: None object
    """

    print('\nThe function "do_import()" is running...')
    try:
        path_to_import = neo4j_path + '/import/HD'
        # stop neo4j
        cmd = '{}/bin/neo4j stop'.format(neo4j_path)
        subprocess.call(cmd, shell=True)
        # rm any database in the database dir
        if os.path.isdir('{}/data/databases/graph.db'.format(neo4j_path)):
            cmd = 'rm -rf {}/data/databases/graph.db/*'.format(neo4j_path)
            subprocess.call(cmd, shell=True)
        # cd import dir files path
        cmd = 'cd {}'.format(path_to_import)
        subprocess.call(cmd, shell=True)
        # neo4j-import
        cmd = '{}/bin/neo4j-admin database import full --id-type=string --overwrite-destination=true	 ' \
              '--nodes {}/HD_concepts.csv ' \
              '--relationships {}/HD_statements.csv'.format(neo4j_path, path_to_import, path_to_import)
        subprocess.call(cmd, shell=True)
        # start neo4j from database dir
        cmd = 'cd {}/data/databases/graph.db'.format(neo4j_path)
        subprocess.call(cmd, shell=True)
        cmd = '{}/bin/neo4j restart'.format(neo4j_path)
        subprocess.call(cmd, shell=True)
        # wait for 5 seconds and check
        time.sleep(30)
        if os.path.isfile('{}/run/neo4j.pid'.format(neo4j_path)):
            neo4j_msg = 'Neo4j is running.'
        else:
            neo4j_msg = 'Neo4j is NOT running. Some problem occurred and should be checked.'
    except:
        print('error: {}'.format(sys.exc_info()[0]))
        raise
    else:
        return print('\nThe graph is imported into the server. {}'
                     'You can start exploring and querying for hypothesis. \n'.format(neo4j_msg))

if __name__ == '__main__':
    create_neo4j_instance()
    ## get edges and files for neo4j
    edges = get_dataframe_from_file('./graph/graph_edges_v2022-07-24.csv')
    nodes = get_dataframe_from_file('./graph/graph_nodes_v2022-07-24.csv')
    statements = get_statements(edges)    
    concepts = get_concepts(nodes)

    ## import the graph into neo4j
    # save files into neo4j import dir
    neo4j_path = './neo4j-community-5.1.0'
    save_neo4j_files(statements, neo4j_path, file_type='statements')
    save_neo4j_files(concepts, neo4j_path, file_type='concepts')

    # import graph into neo4j
    do_import(neo4j_path)
