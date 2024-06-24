from shutil import rmtree
from pathlib import Path
from sys import getsizeof
from os.path import exists
from os import mkdir, getcwd
from hashlib import sha256

from tqdm import tqdm
from neo4j import GraphDatabase
from neo4j.spatial import Point
from neo4j.time import DateTime, Date, Time, Duration
from neo4j.exceptions import ServiceUnavailable

from ._backends import to_json, get_unique_prop_key


class Extractor:

    def __init__(self, project_dir, driver: GraphDatabase.driver, database: str = "neo4j", input_yes: bool = False,
                 compress: bool = True, indent_size: int = 0, pull_uniqueness_constraints: bool = True,
                 json_file_size: int = int("0xFFFF", 16)):

        """
        The purpose of this class is to extract all the information from a neo4j graph

        :param project_dir: The directory where to back up Neo4j Graph
        :param driver: Neo4j driver
        :param input_yes: bool, determines weather to just type in "y" for all input options
        :param compress: bool, weather or not to compress files as they are being extracted
        :param json_file_size: int, max size of json object in memory before dumping
        :param pull_uniqueness_constraints: bool, bool weather or not to extract constraints
        """

        self.project_dir: Path = Path(getcwd()) / project_dir
        self.data_dir: Path = self.project_dir / 'data'
        self.driver: GraphDatabase.driver = driver
        self.database: str = database
        self.input_yes: bool = input_yes
        self.compress: bool = compress
        self.indent_size: int = indent_size
        self.pull_uniqueness_constraints: bool = pull_uniqueness_constraints

        self.property_keys: set = set()
        self.labels: set = set()
        self.rel_types: set = set()
        self.uniqueness_constraints: list = []
        self.db_id: str = ""

        self.node_ids: set = set()
        self.working_extracted_nodes: list = list()
        self.working_extracted_rel: list = list()
        self.node_counter: int = 0
        self.rel_counter: int = 0

        self.uniqueness_constraints_names: list = []

        self.json_file_size: int = json_file_size  # Default size of json objects in memory

    def extract_data(self):

        # Extracts data

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

        if self.pull_uniqueness_constraints:
            self._pull_constraints()  # get constraints of database

        self._pull_relationships()  # get relationship in database
        self._pull_lonely_nodes()  # get nodes that are lonely in database

        # dump and compress remaining data
        if self.working_extracted_nodes:
            to_json(self.data_dir / f"nodes_{self.node_counter}.json", self.working_extracted_nodes, compress=self.compress,
                    indent=self.indent_size)
        if self.working_extracted_rel:
            to_json(self.data_dir / f"relationships_{self.rel_counter}.json", self.working_extracted_rel, compress=self.compress,
                    indent=self.indent_size)

        # calculate a unique prop key to act a dummy id prop for importing
        unique_prop_key = self._calc_unique_prop_key()

        # Store meta data
        to_json(file_path=self.project_dir / f"db_id.json", data=self.db_id)
        to_json(file_path=self.project_dir / f"unique_prop_key.json", data=unique_prop_key)
        to_json(file_path=self.project_dir / f"uniqueness_constraints.json", data=self.uniqueness_constraints)
        to_json(file_path=self.project_dir / "property_keys.json", data=list(self.property_keys))
        to_json(file_path=self.project_dir / "node_labels.json", data=list(self.labels))
        to_json(file_path=self.project_dir / "rel_types.json", data=list(self.rel_types))
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

            # Older verisons
            try:
                results = session.run("CALL db.constraints")
                for result in results:

                    # Get raw constraint string
                    constraint_description = dict(result)['description']

                    # Verify is uniqueness constraint
                    if "unique" in constraint_description:
                        # Get the node label
                        node_label = constraint_description.split(":")[1]
                        node_label = node_label.split(")")[0].strip()

                        # Get the node property
                        node_prop = constraint_description.split(".")[1]
                        node_prop = node_prop.split(")")[0].strip()

                        constraint_name = dict(result)['name']
                        constraint = dict(
                            node_label=node_label,
                            node_prop=node_prop,
                            constraint_name=constraint_name,
                        )
                        self.uniqueness_constraints.append(constraint)
                        self.uniqueness_constraints_names.append(constraint_name)

            # Newer verisons
            except:
                results = session.run("SHOW CONSTRAINTS")
                for result in results:
                    if result["type"] == "UNIQUENESS" and result["entityType"] == "NODE":
                        constraint_name = result["name"]
                        for node_label in result["labelsOrTypes"]:
                            for node_prop in result["properties"]:
                                constraint = dict(
                                    node_label=node_label,
                                    node_prop=node_prop,
                                    constraint_name=constraint_name,
                                )
                                self.uniqueness_constraints.append(constraint)
                                self.uniqueness_constraints_names.append(constraint_name)

    @staticmethod
    def __hash_props(props):
        literals = ["$point(", "$date(", "$time(", "$datetime(", "$duration("]

        hash_props = {}
        for prop_key, prop_value in props.items():
            if isinstance(prop_value, str) and any(prop_value.startswith(s) for s in literals):
                prop_hash = sha256(prop_value.encode('utf-8')).hexdigest()
                hash_props[prop_key] = prop_value
                props[prop_key] = prop_hash

        return hash_props, props

    @staticmethod
    def __parse_props(props):

        # Custom Parser
        def __parse_prop(prop):

            # Treat points seperately
            if isinstance(prop, Point):
                point_srid = prop.srid
                prop = list(prop)
                if point_srid == 7203:
                    prop = f"$point({'{'}x: {prop[0]}, y: {prop[1]}, crs: 'cartesian'{'}'})"
                elif point_srid == 9157:
                    prop = f"$point({'{'}x: {prop[0]}, y: {prop[1]}, z: {prop[2]}, crs: 'cartesian-3d'{'}'})"
                elif point_srid == 4326:
                    prop = f"$point({'{'}x: {prop[0]}, y: {prop[1]}, crs: 'wgs-84'{'}'})"
                elif point_srid == 4979:
                    prop = f"$point({'{'}x: {prop[0]}, y: {prop[1]}, z: {prop[2]}, crs: 'wgs-84-3d'{'}'})"
                else:
                    raise ValueError(f"Point of srid {point_srid} is not supported")

            # Treat temporal values seperately
            elif isinstance(prop, Date):
                prop = f"$date('{prop.iso_format()}')"
            elif isinstance(prop, Time):
                prop = f"$time('{prop.iso_format()}')"
            elif isinstance(prop, DateTime):
                prop = f"$datetime('{prop.iso_format()}')"
            elif isinstance(prop, Duration):
                prop = f"$duration('{prop.iso_format()}')"

            # Otherwise, return the prop
            return prop

        for prop_key, prop_value in props.items():

            # Treat each item in an array
            if isinstance(prop_value, list):
                prop_values = []
                for sub_prop_value in prop_value:
                    prop_value = __parse_prop(sub_prop_value)
                    prop_values.append(prop_value)
                props[prop_key] = prop_values

            else:
                prop_value = __parse_prop(prop_value)
                props[prop_key] = prop_value

        return props

    def __parse_node__(self, node):
        # Grab important items from a node object
        node_id = node.id
        node_labels = list(node.labels)
        node_labels = sorted(node_labels, key=str.lower)
        node_labels = ":".join(node_labels)
        node_props = dict(node)
        return node_id, node_labels, node_props
    
    def _update_node(self, node):

        # Updates the current working nodes
        node_id, node_labels, node_props = self.__parse_node__(node)
        
        # Only update if node does not already updated
        if node_id not in self.node_ids:

            hash_props, node_props = self.__hash_props(node_props)
            node_props = self.__parse_props(node_props)

            self.node_counter += 1

            row = {'node_id': node_id, 'node_labels': node_labels,
                   'node_props': node_props, 'hash_props': hash_props}

            self.working_extracted_nodes.append(row)

            self.node_ids.add(node_id)
            self.property_keys.update(node_props.keys())
            self.labels.add(node_labels)

            if ":" in node_labels:
                for node_label in node_labels.split(":"):
                    self.labels.add(node_label)

            size_in_ram = getsizeof(self.working_extracted_nodes)
            if size_in_ram > self.json_file_size:
                to_json(self.data_dir / f"nodes_{self.node_counter}.json", self.working_extracted_nodes, compress=self.compress,
                        indent=self.indent_size)
                self.working_extracted_nodes = []

    def _update_rel(self, rel, start_node, end_node):
        # Gather relationship
        rel_type = rel.type
        rel_props = dict(rel)
        hash_props, rel_props = self.__hash_props(rel_props)
        rel_props = self.__parse_props(rel_props)

        self.property_keys.update(rel_props.keys())
        self.rel_types.add(rel_type)

        row = {'rel_id': self.rel_counter,
                'start_node_id': start_node.id,
                'end_node_id': end_node.id,
                'rel_type': rel_type,
                'rel_props': rel_props,
                'hash_props': hash_props}
        self.working_extracted_rel.append(row)

        self.rel_counter += 1

        size_in_ram = getsizeof(self.working_extracted_rel)
        if size_in_ram > self.json_file_size:
            to_json(self.data_dir / f"relationships_{self.rel_counter}.json", self.working_extracted_rel, compress=self.compress,
                    indent=self.indent_size)
            self.working_extracted_rel = []
    
    def _pull_relationships(self):

        query = """
        MATCH (sn)-[r]->(en)
        RETURN sn, en, r
        """

        with self.driver.session(database=self.database) as session:
            number_of_relationships = session.run("MATCH p=(sn)-[r]->(en) RETURN COUNT(p)").value()[0]
            results = session.run(query)

            for record in tqdm(results, total=number_of_relationships,
                               desc="Extracting Relationships"):
                start_node = record['sn']
                end_node = record['en']
                rel = record["r"]

                self._update_node(start_node)
                self._update_node(end_node)
                self._update_rel(rel, start_node, end_node)

    def _pull_lonely_nodes(self):

        query = """
        MATCH (n)
        WHERE NOT EXISTS((n)-[]-())
        RETURN n
        """

        with self.driver.session(database=self.database) as session:
            number_of_nodes = session.run(f"MATCH (n) WHERE NOT EXISTS((n)-[]-()) RETURN COUNT(n)").value()[0]
            results = session.run(query)
            for record in tqdm(results, total=number_of_nodes, desc="Extracting Lonely Nodes"):
                # Base node object
                node = record['n']
                self._update_node(node)

    def _calc_unique_prop_key(self):
        keys_to_avoid = self.property_keys.copy()
        keys_to_avoid.update(self.uniqueness_constraints_names)

        # Neo4j's built in IDs can change as new entities are added. So, a unique property is generated where the
        # pulled ids are placed temporarily. A unique property is calculated because we do not want to 'create' a
        # dummy property that the user actually uses.
        unique_prop_key = get_unique_prop_key(keys_to_avoid)
        return unique_prop_key
