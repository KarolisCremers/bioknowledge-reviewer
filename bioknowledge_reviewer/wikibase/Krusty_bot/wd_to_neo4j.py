import configargparse
import pandas as pd
from tqdm import tqdm
from wikidataintegrator import wdi_core, wdi_helpers
import more_itertools
import re
import neo4jlib
from utils import *

class Bot:
    edge_columns = [':START_ID', ':TYPE', ':END_ID', 'reference_uri', 'reference_supporting_text',
                    'reference_date', 'property_label', 'property_description:IGNORE', 'property_uri']
    node_columns = ['id:ID', ':LABEL', 'preflabel', 'synonyms:IGNORE', 'name', 'description']

    def __init__(self, sparql_endpoint_url, mediawiki_api_url, node_out_path,
                 edge_out_path, update_neo4j, update_path):
        self.update_noe4j = bool(update_neo4j)
        if self.update_noe4j:
            self.update_path = update_path
        self.sparql_endpoint_url = sparql_endpoint_url
        self.mediawiki_api_url = mediawiki_api_url
        self.node_out_path = node_out_path
        self.edge_out_path = edge_out_path
        self.equiv_prop_pid = ""
        uri_pid = wdi_helpers.id_mapper(self.get_equiv_prop_pid(), endpoint=sparql_endpoint_url)
        self.pid_uri = {v: k for k, v in uri_pid.items()}
        dbxref_pid = uri_pid['http://www.geneontology.org/formats/oboInOwl#DbXref']
        dbxref_qid = wdi_helpers.id_mapper(dbxref_pid, endpoint=sparql_endpoint_url)
        self.qid_dbxref = {v: k for k, v in dbxref_qid.items()}
        self.ref_supp_text_pid = uri_pid["http://reference_supporting_text"]
        self.reference_uri_pid = uri_pid["http://www.wikidata.org/entity/P854"]
        self.type_pid = uri_pid["http://type"]

        # prop label and descriptions
        pids = {x for x in self.qid_dbxref if x.startswith("P")}
        props = wdi_core.WDItemEngine.generate_item_instances(list(pids), mediawiki_api_url)
        self.pid_label = {pid: item.get_label() for pid, item in props}
        self.pid_descr = {pid: item.get_description() for pid, item in props}

        # get all items and all statements
        qids = {x for x in self.qid_dbxref if x.startswith("Q")}
        self.item_iter = self.item_chunker(sorted(list(qids)))
        # self.item_iter = self.item_chunker(['Q94', "Q347"])

        self.edge_lines = []
        self.node_lines = []
        self.neo4j_nodes = []
        self.neo4j_statements = []

    def item_chunker(self, qids) -> wdi_core.WDItemEngine:
        # iterate through item instances, getting 20 at a time
        chunks = more_itertools.chunked(qids, 20)
        for chunk in chunks:
            items = wdi_core.WDItemEngine.generate_item_instances(chunk, mediawiki_api_url=self.mediawiki_api_url)
            for item in items:
                yield item[1]
    

    @staticmethod
    def undo_id_parenthesis(s):
        # example "N-Acetyl-D-glucosamine (CHEBI:17411)" -> "N-Acetyl-D-glucosamine"
        if " (" in s and s.endswith(")"):
            idx1 = s.rindex(" (")
            s = s[:idx1]
        return s
    
    
    def update(self):
        """
        Creates a new local instanc of NEO4J
        that uses the downloaded data.
        """
        if not self.update_noe4j:
            return
        else:
            # create a Neo4j server instance
            neo4j_dir = neo4jlib.create_neo4j_instance('5.1.0')
            print('The name of the neo4j directory is {}'.format(neo4j_dir))
            df_edges = pd.read_csv(self.edge_out_path)
            df_nodes = pd.read_csv(self.node_out_path)
            # import to graph database
            
            ## save files into neo4j import dir
            neo4j_path = './{}'.format(neo4j_dir)
            neo4jlib.save_neo4j_files(df_edges, neo4j_path, file_type = 'statements')
            neo4jlib.save_neo4j_files(df_nodes, neo4j_path, file_type = 'concepts')
            
            ## import graph into neo4j database
            neo4jlib.do_import(neo4j_path)


    def write_out(self):
        """
        This function writes the downloaded data
        into 2 different files, a node file and a edge file.
        """
        for line in self.edge_lines:
          for key, value in line.items():
            if type(value) is str and value == '':
              line[key] = None
        df_edges = pd.DataFrame(self.edge_lines)
        df_edges['reference_date'] = None
        df_edges = df_edges[self.edge_columns]
        df_edges.fillna('NA').to_csv(self.edge_out_path, index=None)

        for line in self.node_lines:
          for key, value in line.items():
            if type(value) is str and value == '':
              line[key] = None
        df_nodes = pd.DataFrame(self.node_lines)
        df_nodes = df_nodes[self.node_columns]
        df_nodes.fillna('NA').to_csv(self.node_out_path, index=None)
            
            
    def reform_edge_format(self, edges_lines, xrefs):
        """
        This function changes node id's used within
        the edges to the ones originally used in NEO4J

        Parameters
        ----------
        edges_lines : list
            List of statements.
        xrefs : dict
            Dictionary with key= wikibase id, value= neo4j id.

        Returns
        -------
        new_edges : list
            List of updated statements.
        """
        new_edges = []
        for edge in edges_lines:
            alt_start_id = xrefs[edge[":START_ID"]]
            alt_end_id = xrefs[edge[":END_ID"]]
            edge[":START_ID"] = alt_start_id
            edge[":END_ID"] = alt_end_id
            new_edges.append(edge)
        return new_edges
        
        
    def get_equiv_prop_pid(self):
        """
        This function queries the Wikibase for the
        property id (Px) of the 'equivalent property'
        property.
        This id is used for the extraction of all
        property (Px) and item (Qx) id's stored
        within the Wikibase.
        This function is automatically called when generating
        a Bot object.

        Returns
        -------
        String
            Property id (Px) assigned to 'equivalent property'
            within the Wikibase server.
        """
        if self.equiv_prop_pid:
            return self.equiv_prop_pid
        # get the equivalent property property without knowing the PID for equivalent property!!!
        query = '''SELECT * WHERE {
          ?item ?prop <http://www.w3.org/2002/07/owl#equivalentProperty> .
          ?item <http://wikiba.se/ontology#directClaim> ?prop .
        }'''
        pid = wdi_core.WDItemEngine.execute_sparql_query(query, endpoint=self.sparql_endpoint_url)
        pid = pid['results']['bindings'][0]['prop']['value']
        pid = pid.split("/")[-1]
        self.equiv_prop_pid = pid
        return self.equiv_prop_pid 
    
    
    def parse_gene(self, node):
        """
        Parses nodes that are likely to be a gene.
        Handles different ways of how a gene title
        may be stored as.

        Parameters
        ----------
        node : dict
            A Wikibase item dict that describes a concept.

        Returns
        -------
        node : dict
            A Wikibase item dict that describes a concept.
        idxref : tuple
            Name used in NEO4J, Name used in Wikibase.
        """
        packed_id = node["id:ID"]
        unpacked_id = (node["id:ID"].split(" "))
        if len(unpacked_id) == 2:
            preflabel, node_id = unpacked_id
            if "HGNC:" in node_id:
                stripped_node_id = re.sub("\(|\)|\[|\]|\'", "", node_id)
                if preflabel == "NA":
                    node["id:ID"] = stripped_node_id
                    node["preflabel"] = stripped_node_id
                else:
                    node["id:ID"] = stripped_node_id
                    node["preflabel"] = preflabel       
            else:
                node["id:ID"] = node["preflabel"]
                node["preflabel"] = packed_id
        else:
            node["id:ID"] = node["preflabel"]
            node["preflabel"] = re.sub("\(|\)|\[|\]|\'", "", packed_id)
        idxref = (packed_id, node["id:ID"])
        return node, idxref
    
    
    def parse_long_form(self, node):
        """
        Parse nodes with extended ids or item titles
        and put information in the correct columns.
        
        Parameters
        ----------
        node : dict
            A Wikibase item dict that describes a concept.

        Returns
        -------
        node : dict
            A Wikibase item dict that describes a concept.
        idxref : Tuple
            Name used in NEO4J, Name used in Wikibase.
        """
        packed_id = node["id:ID"]
        node["id:ID"] = node["preflabel"]
        node["preflabel"] = packed_id
        node["name"] = packed_id
        idxref = (packed_id, node["id:ID"])
        return node, idxref
    
    def reformat_node(self, node):
        """
        This function destinguishes different
        node types and uses an appropriate function
        to reformat the information into NEO4J
        format.

        Parameters
        ----------
        node : dict
            A Wikibase item dict that describes a concept.

        Returns
        -------
        node : dict
            A reformatted item dictionary corresponding to
            correct NEO4J csv import format.
        idxref : Tuple
            Name used in NEO4J, Name used in Wikibase.

        """
        #TODO when updating script to python 3.10 or higher change to switch-case function
        node_type = node[":LABEL"]
        if node_type == "GENE":
            node, idxref = self.parse_gene(node)
        elif node_type == "NA":
            node, idxref = self.parse_gene(node)
        elif node_type == "GENO":
            node, idxref = self.parse_long_form(node)
        elif node_type == "DISO":
            node, idxref = self.parse_long_form(node)
        elif node_type == "PHYS":
            node, idxref = self.parse_long_form(node)
        elif node_type == "VARI":
            node, idxref = self.parse_long_form(node)
        elif node_type == "ANAT":
            node, idxref = self.parse_long_form(node)
        else:
            print("Not a label")
            return None
        return node, idxref

        
    def parse_node(self, item: wdi_core.WDItemEngine):
        """
        This function extracts concept information from
        a given item object.

        Parameters
        ----------
        item : wdi_core.WDItemEngine
            Stored concept information in a wikibasedataintegrator
            item object.

        Returns
        -------
        node_template : dictionary
            Concept/node information from the item object.
        xref : Tuple
            Name used in NEO4J, Name used in Wikibase.

        """
        type_statements = [s for s in item.statements if s.get_prop_nr() == self.type_pid]
        if len(type_statements) != 1:
            return None
        node_template = dict()
        node_template[':LABEL'] = self.qid_dbxref["Q" + str(type_statements[0].get_value())]
        node_template['id:ID'] = self.qid_dbxref[item.wd_item_id]
        node_template['preflabel'] = self.undo_id_parenthesis(item.get_label())
        node_template['name'] = item.get_label()
        node_template['description'] = item.get_description()
        node_template['synonyms:IGNORE'] = "|".join(item.get_aliases())
        node_template, xref = self.reformat_node(node_template)
        return (node_template, xref)
        
    def handle_statement(self, s, start_id):
        """
        This function 

        Parameters
        ----------
        s : statement/edge
            Wikibase object containing statement information.
        start_id : String
            Start id or source Qid or Pid of the relationship.

        Returns
        -------
        edge_lines : List
            A list of edge objects in the NEO4J format.

        """
        # if a statement has multiple refs, it will return multiple lines
        skip_statements = {
            "http://www.geneontology.org/formats/oboInOwl#DbXref",
            "http://type"
        }
        edge_lines = []
        #TODO Change id here?
        line = {":START_ID": start_id, 'property_uri': self.pid_uri[s.get_prop_nr()]}
        if line['property_uri'] in skip_statements:
            return edge_lines
        line['property_label'] = self.pid_label[s.get_prop_nr()]
        line['property_description:IGNORE'] = self.pid_descr[s.get_prop_nr()]
        line[':TYPE'] = self.qid_dbxref[s.get_prop_nr()]
        #TODO Change id here?
        line[':END_ID'] = self.qid_dbxref["Q" + str(s.get_value())] if s.data_type == "wikibase-item" else s.get_value()
        if s.references:
            for ref in s.references:
                ref_supp_text_statements = [x for x in ref if x.get_prop_nr() == self.ref_supp_text_pid]
                ref_supp_text = " ".join([x.get_value() for x in ref_supp_text_statements])
                reference_uri_statements = [x for x in ref if x.get_prop_nr() == self.reference_uri_pid]
                reference_uri = "|".join([x.get_value() for x in reference_uri_statements])
                # todo: rejoin split pubmed urls
                line['reference_supporting_text'] = ref_supp_text
                line['reference_uri'] = reference_uri
                edge_lines.append(line.copy())
        else:
            edge_lines.append(line.copy())
        return edge_lines
    

    def run(self):
        """
        This function downloads the Structured Review
        from the Wikibase and formats it into
        NEO4J formatted import files.
        """
        edge_lines = []
        node_lines = []
        xrefs = {}
        for item in tqdm(self.item_iter):
            sub_qid = item.wd_item_id
            start_id = self.qid_dbxref[sub_qid]
            for s in item.statements:
                edge_lines.extend(self.handle_statement(s, start_id))
            node_output = self.parse_node(item)
            if node_output:
                node_lines.append(node_output[0].copy())
                xrefs[node_output[1][0]] = node_output[1][1]
        self.edge_lines = self.reform_edge_format(edge_lines, xrefs)
        self.node_lines = node_lines
        self.write_out()
        self.update()


def main(mediawiki_api_url, sparql_endpoint_url, node_out_path, edge_out_path, update_neo4j, update_path):
    bot = Bot(sparql_endpoint_url, mediawiki_api_url, node_out_path, edge_out_path, update_neo4j, update_path)
    bot.run()


if __name__ == '__main__':
    p = configargparse.ArgParser(default_config_files=['config.cfg'])
    p.add('-c', '--config', is_config_file=True, help='config file path')
    p.add("--mediawiki_api_url", required=True, help="Wikibase mediawiki api url")
    p.add("--sparql_endpoint_url", required=True, help="Wikibase sparql endpoint url")
    p.add("--node-out-path", required=True, help="path to output neo4j nodes csv")
    p.add("--edge-out-path", required=True, help="path to output neo4j edges csv")
    p.add("--update-neo4j", required=True, help="should the local NEO4J" + 
          " instance be updated after download?")
    p.add("--update-path", required=False, help="path to the main project folder") #TODO better words
    options, _ = p.parse_known_args()
    print(options)
    d = options.__dict__.copy()
    del d['config']
    main(**d)
