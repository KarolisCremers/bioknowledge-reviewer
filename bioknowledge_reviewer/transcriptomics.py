# @name: transcriptomics.py
# @description: Module for RNA-seq expression network preparation and management
# @version: 1.0
# @date: 21-01-2019
# @author: Núria Queralt Rosinach
# @email: nuriaqr@scripps.edu

# TODO: check dir structure for input and data
"""Module for the transcriptomics data"""

import datetime
import pandas as pd
import os
from biothings_client import get_client
from Node import Node
# VARIABLES
today = datetime.date.today()

# path to write data
path = os.getcwd() + "/transcriptomics"
if not os.path.isdir(path): os.makedirs(path)


# CHECK NETWORK SCHEMA AND NORMALIZE TO GRAPH SCHEMA
# check network schema
# TODO: check functions


def read_data(csv_path, sep):
    """
    This function reads the raw differential gene expression data from a CSV file.
    :param csv_path: path to gene expression CSV file string, \
     e.g. 'home/rna-seq/ngly1-fly-chow-2018/data/supp_table_1.csv'
    :return: rna dataframe
    """

    print('\nThe function "read_data()" is running...')
    # import table S1 (dNGLY1 KO - transcriptomic profile)
    #csv_path = '~/workspace/ngly1-graph/regulation/ngly1-fly-chow-2018/data/supp_table_1.csv'
    data_df = pd.read_csv('{}'.format(csv_path), sep=sep)
    print('\n* This is the size of the raw expression data structure: {}'.format(data_df.shape))
    print('* These are the expression attributes: {}'.format(data_df.columns))
    print('* This is the first record:\n{}'.format(data_df.head(1)))

    # save raw data
    path = os.getcwd() + '/transcriptomics/HD/data'
    if not os.path.isdir(path): os.makedirs(path)
    if not os.path.exists('{}/GSE64810_mlhd_DESeq2_diffexp_DESeq2_outlier_trimmed_adjust.csv'.format(path)):
        data_df.to_csv('{}/GSE64810_mlhd_DESeq2_diffexp_DESeq2_outlier_trimmed_adjust.csv'.format(path), index=False)
    print('\nThe raw data is saved at: {}/GSE64810_mlhd_DESeq2_diffexp_DESeq2_outlier_trimmed_adjust.csv\n'.format(path))
    print('\nFinished read_data().\n')
    data_df = data_df.rename(columns={'Unnamed: 0': 'EnsembleID'})
    return data_df


def clean_data(data_df):
    """
    This function cleans the raw data structure to the expression attributes of interest to build the graph.
    :param data_df: rna dataframe from the read_data() function
    :return: expression dataframe
    """

    print('\nThe function "clean_data()" is running. Keeping only data with FC > 1.5 and FDR < 5% ...')
    # subset [FC 1.5, FDR 5%] (386 = sum(96,290))
    up = data_df.query('log2FoldChange >= 0.57 and padj <= 0.05')
    down = data_df.query('log2FoldChange <= -0.57 and padj <= 0.05')
    up = (up
          [['EnsembleID', 'symbol', 'log2FoldChange', 'pvalue', 'padj']]
          # .rename(columns={'log2FoldChange': 'log2FC', 'padj': 'FDR'})
          .reset_index(drop=True)
          .assign(Regulation='Upregulated')
          )
    #up.sort_values(by='log2FoldChange', ascending=False).head(1)
    down = (down
            [['EnsembleID', 'symbol', 'log2FoldChange', 'pvalue', 'padj']]
            # .rename(columns={'log2FoldChange': 'log2FC', 'padj': 'FDR'})
            .reset_index(drop=True)
            .assign(Regulation='Downregulated')
            )
    #down.sort_values(by='log2FoldChange', ascending=True).head(1)
    subset_df = pd.concat([up, down])
    print('\n* This is the size of the clean expression data structure: {}'.format(subset_df.shape))
    print('* These are the clean expression attributes: {}'.format(subset_df.columns))
    print('* This is the first record:\n{}'.format(subset_df.head(1)))

    # save subset
    path = os.getcwd() + '/transcriptomics/HD/out'
    if not os.path.isdir(path): os.makedirs(path)
    subset_df.to_csv('{}/Transcriptome_human_BA9.csv'.format(path), index=False)
    print('\nThe clean data is saved at: {}/Transcriptome_human_BA9.csv\n'.format(path))
    print('\nFinished clean_data().\n')

    return subset_df


def prepare_data_edges(chow):
    """
    This function prepares the expression dataset as edges.
    :param chow: expression dataframe from the clean_data() function
    :return: edges dataframe
    """

    print('\nThe function "prepare_data_edges()" is running...')
    # read dataset
    # csv_path = os.getcwd() + '/transcriptomics/ngly1-fly-chow-2018/out/fc1.5_fdr5_transcriptome_fly.csv'
    # chow = pd.read_csv('{}'.format(csv_path))
    #TODO: discuss edge property
    # prepare edges
    chow = (chow
            .rename(columns={'EnsembleID': 'ensemble_id', 'Regulation': 'regulation'})
            .assign(source='Chow')
            .assign(subject_id='MONDO:0007739')
            .assign(subject_label='Huntington disease')
            .assign(property_id='MI_0914')
            .assign(property_label='association')
            .assign(reference_id='PMID:27454300')
            .assign(reference_uri='http://purl.obolibrary.org/obo/MI_0914')
            .assign(reference_supporting_text="Here we present a genome-wide analysis of mRNA expression in human prefrontal cortex from 20 HD and 49 neuropathologically normal controls using next generation high-throughput sequencing. Surprisingly, 19% (5,480) of the 28,087 confidently detected genes are differentially expressed (FDR<0.05) and are predominantly up-regulated. A novel hypothesis-free geneset enrichment method that dissects large gene lists into functionally and transcriptionally related groups discovers that the differentially expressed genes are enriched for immune response, neuroinflammation, and developmental genes. Markers for all major brain cell types are observed, suggesting that HD invokes a systemic response in the brain area studied. ")
            .assign(reference_date="25/7/2016")
            .assign(property_description="Interaction between molecules that may participate in formation of one, but possibly more, physical complexes. Often describes a set of molecules that are co-purified in a single pull-down or coimmunoprecipitation but might participate in formation of distinct physical complexes sharing a common bait.")
            .assign(property_uri="http://purl.obolibrary.org/obo/INO_0000061")
            )
    chow['object_id'] = chow.ensemble_id.apply(lambda x: 'ensembl:' + str(x).split(".")[0])
    
    # save individual dataset edges
    path = os.getcwd() + '/transcriptomics/HD/out'
    if not os.path.isdir(path): os.makedirs(path)
    chow.to_csv('{}/Transcriptome_human_BA9_edges.csv'.format(path), index=False)
    print('\n* This is the size of the expression data structure: {}'.format(chow.shape))
    print('* These are the expression attributes: {}'.format(chow.columns))
    print('* This is the first record:\n{}'.format(chow.head(1)))
    print('\nThe HD BA9 transcriptomics expression edges are saved at:'
          ' {}/Transcriptome_human_BA9_edges.csv\n'.format(path))
    print('\nFinished prepare_data_edges().\n')

    return chow


def prepare_rna_edges(chow):
    """
    This function prepares and compiles all individual data edges into RNA edges to build the graph.
    :param chow: edges dataframe from the prepare_data_edges() function
    :return: network dataframe
    """

    print('\nThe function "prepare_rna_edges()" is running...')
    # read individual datasets
    # csv_path = os.getcwd() + '/transcriptomics/ngly1-fly-chow-2018/out/chow_fc1.5_fdr5_transcriptome_fly_edges.csv'
    # chow = pd.read_csv('{}'.format(csv_path))

    # select and rename key columns
    chow = (chow
            [['symbol', 'log2FoldChange', 'pvalue', 'padj',
              'regulation', 'source', 'subject_id', 'subject_label', 'property_id',
              'property_label', 'reference_id', 'object_id',
              'reference_uri','reference_supporting_text','reference_date',
              'property_description', 'property_uri']]
            .rename(columns={'symbol': 'object_label', 'padj': 'fdr'})

            )

    # reorder columns
    chow = chow[['subject_id', 'subject_label', 'property_id',
                 'property_label', 'object_id', 'object_label', 'log2FoldChange', 'pvalue', 'fdr', 'regulation',
                 'source', 'reference_id',
                 'reference_uri','reference_supporting_text','reference_date',
                 'property_description', 'property_uri']]
    # concat edges
    edges = pd.concat([chow, pd.DataFrame()], ignore_index=True)

    # drop duplicates
    edges.drop_duplicates(inplace=True)

    # print edges info
    print('\n* This is the size of the edges data structure: {}'.format(edges.shape))
    print('* These are the edges attributes: {}'.format(edges.columns))
    print('* This is the first record:\n{}'.format(edges.head(1)))
    print('\nThis data object is not saved.\n')
    print('\nFinished prepare_rna_edges().\n')

    return edges


# BUILD NETWORK

def build_edges(edges):
    """
    This function builds the edges network with the graph schema.
    :param edges: network dataframe from the prepare_rna_edges() function
    :return: graph edges object as a list of dictionaries, where every dictionary is a record
    """

    print('\nThe function "build_edges()" is running...')
    # give graph format
    curie_dct = {
        'ro': 'http://purl.obolibrary.org/obo/',
        'pmid': 'https://www.ncbi.nlm.nih.gov/pubmed/',
        'encode': 'https://www.encodeproject.org/search/?searchTerm='
    }

    edges_l = list()
    for i, row in edges.iterrows():
        # property uri: http://purl.obolibrary.org/obo/RO_0002434
        property_uri = "http://purl.obolibrary.org/obo/INO_0000061"
        if ':' in row['property_id']:
            property_uri = curie_dct[row['property_id'].split(':')[0].lower()] + row['property_id'].replace(':', '_')

        # reference_uri: https://www.ncbi.nlm.nih.gov/pubmed/25416956
        # capture nan or None values, i.e. all possible nulls
        if (isinstance(row['reference_id'], float) and str(row['reference_id']).lower() == 'nan') or row[
            'reference_id'] is None:
            row['reference_id'] = 'NA'
        if ':' not in row['reference_id']:
            reference_uri = row['reference_id']
        else:
            try:
                reference_uri = curie_dct[row['reference_id'].split(':')[0].lower()] + row['reference_id'].split(':')[1]
            except KeyError:
                reference_uri = row['reference_id']
                print('There is a reference curie with and unrecognized namespace:', row['reference_id'])
        # build list of edges as list of dict, i.e a df, where a dict is an edge
        edge = dict()
        edge['subject_id'] = row['subject_id']
        edge['object_id'] = row['object_id']
        edge['property_id'] = row['property_id']
        edge['property_label'] = row['property_label']
        edge['property_description'] = 'NA'
        edge['property_uri'] = property_uri
        edge['reference_uri'] = reference_uri
        edge[
            'reference_supporting_text'] = 'Here we present a genome-wide analysis of mRNA expression in human prefrontal cortex from 20 HD and 49 neuropathologically normal controls using next generation high-throughput sequencing.' if \
        row['source'] == 'Chow' else 'This edge comes from the RNA-seq profile dataset extracted by the XXX Lab YYYY.'
        edge['reference_date'] = '2015-11-04' if row['source'] == 'Chow' else 'NA'
        edges_l.append(edge)

    # save edges file
    path = os.getcwd() + '/graph'
    if not os.path.isdir(path): os.makedirs(path)
    pd.DataFrame(edges_l).fillna('NA').to_csv('{}/rna_edges_v{}.csv'.format(path,today), index=False)

    # print edges info
    print('\n* This is the size of the edges file data structure: {}'.format(pd.DataFrame(edges_l).shape))
    print('* These are the edges attributes: {}'.format(pd.DataFrame(edges_l).columns))
    print('* This is the first record:\n{}'.format(pd.DataFrame(edges_l).head(1)))
    print('\nThe transcriptomics network edges are built and saved at: {}/rna_edges_v{}.csv\n'.format(path,today))
    print('\nFinished build_edges().\n')

    return edges_l


def merge_to_node(concept_dict, gene_info):
    """
    This function combines the dictionary obtained from the edges
    with the results found using mygene.info api.
    (calling ensembl ID's and returning HGNC ID's where possible)
    Uses the Node object to rapidly convert node information into
    a dictionary instance.
    AUTH: Karolis
    """
    node_list = list()
    missing = []
    counter = 0
    noHGNC = 0
    node_dict = {}
    for idx, row in gene_info.iterrows():
        if type(row["notfound"]) is not float: # ID's not found in mygene.info
            counter += 1
            missing.append(idx)
            if ":" in idx: # checks for labels containing prefix 
                node = Node(idx)
                node.preflabel = idx
                node_formatted = node.get_dict()
                node_dict[idx] = node_formatted
                node_list.append(node_formatted)
            else:
                node = Node("ensembl:" + idx)
                node.preflabel = "ensembl:" + idx
                node_formatted = node.get_dict()
                node_dict["ensembl:" + idx] = node_formatted
                node_list.append(node_formatted)
            continue
        if type(row['HGNC']) == float or type(row['HGNC']) == None:
            noHGNC += 1
            node = Node("ensembl:" + idx)
            node.preflabel = "ensembl:" + idx
            if type(row["alias"]) is not float:
                node.synonyms = row["alias"]
            if type(row["summary"]) is not float:
                node.description = row["summary"]
            if type(row["name"]) is not float:
                node.name = row["name"]
            node_formatted = node.get_dict()
            node_dict["ensembl:" + idx] = node_formatted
            node_list.append(node_formatted)
            continue
        node = Node("HGNC:" + row['HGNC'])
        node.semantic_groups = "GENE"
        node.preflabel = concept_dict["ensembl:" + idx]['preflabel']
        node.name = row["name"]
        node.synonyms = '|'.join(list(row['alias'])) if isinstance(
            row['alias'], list) else row['alias']
        node.description = row['summary']
        node_formatted = node.get_dict()
        node_list.append(node_formatted)
        node_dict["ensembl:" + idx] = node_formatted
    print("{} nodes without available information".format(counter))
    if len(missing) > 0:
        print("These nodes have retained their original ID. First nodes is:")
        print(missing[:1])
    print("{} nodes do not have a HGNC id, retained original ID".format(noHGNC))
    return node_list, node_dict


# def convert_edges(edges, gene_info):
#     """
#     Old function that may be reused on another project if necessary.
#     This function converts edge ids into HGNC id's where possible.
#     input:
#         edges: edge dataframe
#         gene_info: return object frome mygeneinfo api
#     returns:
#         none,
#     AUTH: Karolis
#     """
#     converted_edges = list()
#     for idx, row in gene_info.iterrows():
#         if type(row["notfound"]) is not float:
#             #
#             continue
#         if type(row['HGNC']) == float or type(row['HGNC']) == None:
#             noHGNC += 1
#             node = Node("ensembl:" + idx)
#             node.preflabel = "ensembl:" + idx
#             if type(row["alias"]) is not float:
#                 node.synonyms = row["alias"]
#             if type(row["summary"]) is not float:
#                 node.description = row["summary"]
#             if type(row["name"]) is not float:
#                 node.name = row["name"]
#             node_formatted = node.get_dict()
#             node_dict["ensembl:" + idx] = node_formatted
#             node_list.append(node_formatted)
#             continue
#         edge = dict()
#         edge['subject_id'] = row['subject_id']
#         edge['object_id'] = row['object_id']
#         edge['property_id'] = row['property_id']
#         edge['property_label'] = row['property_label']
#         edge['property_description'] = 'NA'
#         edge['property_uri'] = property_uri
#         edge['reference_uri'] = reference_uri
#         edge[
#             'reference_supporting_text'] = 'Here we present a genome-wide analysis of mRNA expression in human prefrontal cortex from 20 HD and 49 neuropathologically normal controls using next generation high-throughput sequencing.' if \
#         row['source'] == 'Chow' else 'This edge comes from the RNA-seq profile dataset extracted by the XXX Lab YYYY.'
#         edge['reference_date'] = '2015-11-04' if row['source'] == 'Chow' else 'NA'
#         edges_l.append(edge)
    
#     with open() as outfile:
#         outfile.write(converted_edges)


def build_nodes(edges):
    """
    This function builds the nodes network with the graph schema.
    :param edges: network dataframe from the prepare_rna_edges() function
    :return: graph nodes object as a list of dictionaries, where every dictionary is a record
    """

    print('\nThe function "build_nodes()" is running...')
    # retrieve node attributes from biothings and build dictionary
    # from biothings we retrieve: name (new attribute for short description), alias (synonyms), summary (description).
    # symbols in this case come from the original source. otherwise are gonna be retrieved from biothings as well.
    # build concept dict: {id:symbol}
    concept_dct = dict()
    for i, row in edges.iterrows():
        # node for subject
        concept_dct[row['subject_id']] = {'preflabel': row['subject_label']}
        # node for object
        concept_dct[row['object_id']] = {'preflabel': row['object_label']}
    print('* Total number of nodes: {}'.format(len(concept_dct.keys())))

    # biothings api + dictionaries
    # input list for api: since by id we have flybase, hgnc/entrez or ensembl, i am gonna use symbol
    symbols = list()
    #test variable for direct ensemble query:
    ENSID = list()
    for idx, symbol in concept_dct.items():
        # id = key.split(':')[1] if ':' in key else key
        symbols.append(symbol['preflabel'])
        #idx[9:]
        ENSID.append(idx.replace("ensembl:","").split('.')[0])
        
    #print(symbols[0:5])
    #len(symbols)

    # api call
    mg = get_client('gene')
    # symbols, scopes='symbol,alias'
    # ENSID, scopes='ensembl'
    df = mg.querymany(ENSID, scopes='ensembl.gene', fields='alias,name,summary,HGNC', size=1, as_dataframe=True)
    
    nodes_l, node_dict = merge_to_node(concept_dct, df)
    
    #convert_edges(edges, df)

    # save nodes file
    path = os.getcwd() + '/graph'
    if not os.path.isdir(path): os.makedirs(path)
    pd.DataFrame(nodes_l).fillna('NA').to_csv('{}/rna_nodes_v{}.csv'.format(path,today), index=False)

    # print nodes info
    print('\n* This is the size of the nodes file data structure: {}'.format(pd.DataFrame(nodes_l).shape))
    print('* These are the nodes attributes: {}'.format(pd.DataFrame(nodes_l).columns))
    print('* This is the first record:\n{}'.format(pd.DataFrame(nodes_l).head(1)))
    print('\nThe transcriptomics network nodes are built and saved at: {}/rna_nodes_v{}.csv\n'.format(path,today))
    print('\nFinished build_nodes().\n')

    return nodes_l, node_dict


# NETWORK MANAGEMENT FUNCTIONS


def _print_nodes(nodes, filename):
    #TODO: develop this function, also in other edges modules
    """This function save nodes into a CSV file."""

    # print output file

    #return

def rework_edges(edges, nodes):
    """
    This function 
    Parameters
    ----------
    edges : pandas DF
        Dataframe containing edges of the network.
    nodes : dictionary
        Dictionary containing nodes that are contained within
        edges of the input files.
    Returns
    -------
    edges_l : Edge list object
        A list of edges with the node id replaced with HGNC id's
        where possible. List object is used further in other modules
    """
    edges_l = []
    for idx, row in edges.iterrows():
        subject_HGNC = nodes[row["subject_id"]]['id']
        object_HGNC = nodes[row["object_id"]]['id']
        row["subject_id"] = subject_HGNC
        row["object_id"] = object_HGNC
        edges_l.append(row)
    # save edges file
    path = os.getcwd() + '/graph'
    if not os.path.isdir(path): os.makedirs(path)
    pd.DataFrame(edges_l).fillna('NA').to_csv('{}/rna_edges_v{}.csv'.format(path,today), index=False)

    # print edges info
    print('\n* This is the size of the edges file data structure: {}'.format(pd.DataFrame(edges_l).shape))
    print('* These are the edges attributes: {}'.format(pd.DataFrame(edges_l).columns))
    print('* This is the first record:\n{}'.format(pd.DataFrame(edges_l).head(1)))
    print('\nThe transcriptomics network edges are built and saved at: {}/rna_edges_v{}.csv\n'.format(path,today))
    print('\nFinished build_edges().\n')
    return edges_l

if __name__ == '__main__':

    # prepare data to graph schema
    csv_path = '~/LUMC/HDSR/bioknowledge-reviewer/bioknowledge_reviewer/transcriptomics/HD/data/GSE64810_mlhd_DESeq2_diffexp_DESeq2_outlier_trimmed_adjust.csv'
    data = read_data(csv_path, ",")
    clean_data = clean_data(data)
    data_edges = prepare_data_edges(clean_data)
    edges = prepare_rna_edges(data_edges)

    # build network
    transcriptomics_edges = build_edges(edges)
    transcriptomics_nodes, node_dict = build_nodes(edges)
    reworked_edges = rework_edges(edges, node_dict)
    #print('type of edges:', type(transcriptomics_edges))
    #print('str of edges:', transcriptomics_edges)
    #print('type of nodes:', type(transcriptomics_nodes))
    #print('str of nodes:', transcriptomics_nodes)
