from shutil import rmtree
from pathlib import Path
from sys import getsizeof
from os.path import exists
from os import mkdir, getcwd

from tqdm import tqdm
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable

from ._backends import to_json, get_unique_prop_key


class Extractor:

    def __init__(self, project_dir, driver: GraphDatabase.driver, database: str = "neo4j", input_yes: bool = False,
                 compress: bool = True):

        """
        The purpose of this class is to extract all the information from a neo4j graph

        :param project_dir: The directory where to backup Neo4j Graph
        :param driver: Neo4j driver
        :param input_yes: bool, determines weather to just type in "y" for all input options. Be careful when running
            this option
        :param compress: bool, weather or not to compress files as they are being extracted
        """

        self.project_dir: Path = Path(getcwd()) / project_dir
        self.data_dir: Path = self.project_dir / 'data'
        self.driver: GraphDatabase.driver = driver
        self.database: str = database
        self.input_yes: bool = input_yes
        self.compress: bool = compress

        self.property_keys: set = set()
        self.labels: set = set()
        self.rel_types: set = set()
        self.constraints: list = []
        self.constraints_names: list = []
        self.db_id: str = ""

        self.json_file_size: int = int("0xFFFF", 16)  # Default size of json objects in memory

    def extract_data(self):

        self._test_connection()
        self._verify_db_not_empty()

        self._pull_db_id()  # Get ID of database

        if exists(self.project_dir):

            if self.input_yes:
                rmtree(self.project_dir)

            else:
                user_input = input(f"The directory {self.project_dir} already exist, would you like to replace the "
                                   f"directory? (y/N)\n")
                if user_input.lower() == "y":
                    rmtree(self.project_dir)
                else:
                    raise UserWarning("Aborted, project_dir directory already exists")

        mkdir(self.project_dir)
        mkdir(self.data_dir)

        self._pull_constraints()  # get constraints of database
        self._pull_lonely_nodes()  # get nodes in database
        self._pull_relationships()  # get relationship in database

        # calculate a unique prop key to act a dummy id prop for importing
        unique_prop_key = self._calc_unique_prop_key()

        # Store meta data
        to_json(file_path=self.project_dir / f"db_id.json", data=self.db_id)
        to_json(file_path=self.project_dir / f"unique_prop_key.json", data=unique_prop_key)
        to_json(file_path=self.project_dir / f"constraints.json", data=self.constraints)
        to_json(file_path=self.project_dir / f"constraints_names.json", data=self.constraints_names)
        to_json(file_path=self.project_dir / "property_keys.json", data=list(self.property_keys))
        to_json(file_path=self.project_dir / "labels.json", data=list(self.labels))
        to_json(file_path=self.project_dir / "types.json", data=list(self.rel_types))
        to_json(file_path=self.project_dir / "compressed.json", data=self.compress)

    def _test_connection(self):
        try:
            with self.driver.session(database=self.database) as session:
                session.run("MATCH (a) RETURN a LIMIT 1")
        except ServiceUnavailable:
            raise ServiceUnavailable("Unable to connect to database. If this is a local database, make sure the "
                                     "database is running. If this is a remote database, make sure the correct "
                                     "database is referenced.")

    def _verify_db_not_empty(self):
        with self.driver.session(database=self.database) as session:
            results = session.run("MATCH (a) RETURN a LIMIT 1").data()
            if not results:
                raise LookupError("There is not data to pull from the database, make sure the correct database is "
                                  "referenced/running.")

    def _pull_db_id(self):

        with self.driver.session(database=self.database) as session:
            results = session.run("CALL db.info")
            for result in results:
                self.db_id = dict(result)['id']

    def _pull_constraints(self):

        with self.driver.session(database=self.database) as session:
            results = session.run("CALL db.constraints")
            for result in results:
                self.constraints.append(dict(result)['description'])
                self.constraints_names.append(dict(result)['name'])

    @staticmethod
    def __parse_node__(node):
        node_id = node.id
        node_labels = ":".join(list(node.labels))
        node_props = dict(node)
        return node_id, node_labels, node_props

    def _pull_lonely_nodes(self):

        query = """
        
        MATCH (node)
        WHERE NOT (node)-[]-()
        RETURN node
        
        """

        extracted_data = []

        with self.driver.session(database=self.database) as session:
            number_of_nodes = session.run("MATCH (node) WHERE NOT (node)-[]-() RETURN COUNT(node)").value()[0]

            results = session.run(query)
            for index, record in enumerate(tqdm(results, total=number_of_nodes, desc="Extracting Nodes")):

                # Base node object
                node = record['node']
                node_id, node_labels, node_props = self.__parse_node__(node)
                self.property_keys.update(node_props.keys())
                self.labels.add(node_labels)

                row = {'node_id': node_id, 'node_labels': node_labels, 'node_props': node_props}
                extracted_data.append(row)

                if index % 1000 == 0 and index != 0:
                    size_in_ram = getsizeof(extracted_data)
                    if size_in_ram > self.json_file_size:
                        to_json(self.data_dir / f"lonely_nodes_{index}.json", extracted_data, compress=self.compress)
                        extracted_data = []

            # dump and compress remaining data
            if extracted_data:
                to_json(self.data_dir / f"lonely_nodes_{index}.json", extracted_data, compress=self.compress)

    def _pull_relationships(self):

        query = """

        MATCH (start_node)-[rel]->(end_node)
        RETURN start_node, end_node, rel

        """

        extracted_data = []

        with self.driver.session(database=self.database) as session:
            number_of_relationships = session.run("MATCH p=(start_node)-[rel]->(end_node) RETURN COUNT(p)").value()[0]
            results = session.run(query)

            for index, record in enumerate(tqdm(results, total=number_of_relationships,
                                                desc="Extracting Relationships")):

                # Gather starting_node
                start_node = record['start_node']
                start_node_id, start_node_labels, start_node_props = self.__parse_node__(start_node)
                self.property_keys.update(start_node_props.keys())
                self.labels.add(start_node_labels)

                # Gather ending_node
                end_node = record['end_node']
                end_node_id, end_node_labels, end_node_props = self.__parse_node__(end_node)
                self.property_keys.update(end_node_props.keys())
                self.labels.add(end_node_labels)

                # Gather relationship
                rel = record['rel']
                rel_type, rel_props = rel.type, dict(rel)
                self.property_keys.update(rel_props.keys())
                self.rel_types.add(rel_type)

                row = {'start_node_id': start_node_id, 'start_node_labels': start_node_labels,
                       'start_node_props': start_node_props,
                       'end_node_id': end_node_id, 'end_node_labels': end_node_labels,
                       'end_node_props': end_node_props,
                       'rel_type': rel_type, 'rel_props': rel_props}
                extracted_data.append(row)

                if index % 1000 == 0 and index != 0:
                    size_in_ram = getsizeof(extracted_data)
                    if size_in_ram > self.json_file_size:
                        to_json(self.data_dir / f"relationships_{index}.json", extracted_data, compress=self.compress)
                        extracted_data = []

            # dump and compress remaining data
            if extracted_data:
                to_json(self.data_dir / f"relationships_{index}.json", extracted_data, compress=self.compress)

    def _calc_unique_prop_key(self):
        keys_to_avoid = self.property_keys.copy()
        keys_to_avoid.update(self.constraints_names)

        # Neo4j's built in IDs can change as new entities are added. So, a unique property is generated where the
        # pulled ids are placed temporarily. A unique property is calculated because we do not want to 'create' a
        # dummy property that the user actually uses.
        unique_prop_key = get_unique_prop_key(keys_to_avoid)
        return unique_prop_key
