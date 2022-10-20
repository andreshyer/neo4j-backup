from tqdm import tqdm
from pathlib import Path
from os import getcwd

from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable

from ._backends import from_json


class Importer:

    def __init__(self, project_dir, driver: GraphDatabase.driver, database: str = "neo4j", input_yes: bool = False):

        """
        This purpose of this class is to import the information in the project_dir output from the Extractor class.


        :param project_dir: The directory where to backup Neo4j Graph
        :param driver: Neo4j driver
        :param input_yes: bool, determines weather to just type in "y" for all input options. Be careful when running
            this option
        """

        self.project_dir = Path(getcwd()) / project_dir
        self.data_dir: Path = self.project_dir / 'data'
        self.driver: GraphDatabase.driver = driver
        self.database: str = database
        self.input_yes: bool = input_yes

        self.compressed: bool = from_json(self.project_dir / "compressed.json")
        self.unique_prop_key: str = from_json(self.project_dir / f"unique_prop_key.json")
        self.labels: list = from_json(self.project_dir / 'node_labels.json')
        self.relationship_types: list = from_json(self.project_dir / 'rel_types.json')
        self.constraints: list = from_json(self.project_dir / 'constraints.json')

        self.relationships_files = []
        for file_path in self.data_dir.iterdir():
            if "relationship" in file_path.stem:
                file_path = self.data_dir / file_path
                self.relationships_files.append(file_path)

        self.nodes_files = []
        for file_path in self.data_dir.iterdir():
            if "node" in file_path.stem:
                file_path = self.data_dir / file_path
                self.nodes_files.append(file_path)

    def import_data(self):

        """
        This is the main function in this class, this actual imports the data import Neo4j.

        :return:
        """

        self._test_connection()  # Make sure the driver can connect to Neo4j database
        self._verify_is_new_db()  # Make sure database is empty and not the original database
        self._apply_constraints()  # Apply the dummy constraints

        # Grab all the labels used for Nodes
        for file_path in tqdm(self.nodes_files, desc="Importing Nodes"):
            self._import_nodes_file(file_path)

        # Grab all the relationship types used by relationships
        for file_path in tqdm(self.relationships_files, desc="Importing Relationships"):
            self._import_relationships_file(file_path)

        # Remove dummy constraints and properties
        self._cleanup()

    def _test_connection(self):
        try:
            with self.driver.session(database=self.database) as session:
                session.run("MATCH (a) RETURN a LIMIT 1")
        except ServiceUnavailable:
            raise ServiceUnavailable("Unable to connect to database. If this is a local database, make sure the "
                                     "database is running. If this is a remote database, make sure the correct "
                                     "database is referenced.")

    def _verify_is_new_db(self):

        # Grab ping data
        with self.driver.session(database=self.database) as session:
            data = session.run("MATCH (a) RETURN a LIMIT 1").data()

        # If ping data is not empty, then the database is not empty
        if data:

            if self.input_yes:
                raise UserWarning("Aborted, database referenced is not empty")

            else:
                user_input = input("The database referenced is not empty, note inserting data will likely brick the "
                                   "database or create duplicates. "
                                   "Abort? (y/N)\n")
                if user_input == "y":
                    raise UserWarning("Aborted, database referenced is not empty")

    def _apply_constraints(self):

        with self.driver.session(database=self.database) as session:

            # Create dummy constraints on each node label, helps speed up inserting significantly
            for node_labels in tqdm(self.labels, desc='Applying Temporary Constraints'):
                node_labels = node_labels.split(":")
                for node_label in node_labels:
                    constraint = f"CREATE CONSTRAINT {node_label}_{self.unique_prop_key} IF NOT EXISTS ON " \
                                 f"(m:{node_label}) ASSERT (m.{self.unique_prop_key}) IS UNIQUE"
                    session.run(constraint)

            for constraint in tqdm(self.constraints, desc='Applying Actual Constraints', total=len(self.constraints)):
                constraint = constraint.split("CONSTRAINT")[1]
                constraint = "CREATE CONSTRAINT IF NOT EXISTS" + constraint
                session.run(constraint)

    def _import_nodes_file(self, file_path):

        """
        This function might be a bit difficult to understand. One of the fastest way to insert data into Neo4j without
        APOC is with UNWIND statements. Most information can be passed into the cypher through parameters, but
        parameters passed cannot specify a Node Label. There is no way to dynamically allocate node labels in the
        query. So each combination of labels is looped through, and all the nodes in the current working node
        file that have the same label as the current working label are gathered. Then the list of nodes are UNWIND in
        the query.

        Something to note is that is easier and faster to treat nodes with multiple labels as a separate label. Rather
        than taking a node like

        `CREATE (p:Person:Banker {name: "richy"})`

        and trying to merge two separate nodes, one for the Person label and one for the Banker label. Then trying to
        drop the duplicate node and adding the label later. It is easier to look at Person:Banker as separate labels,
        making sure the combination of labels are sorted in alphabetical order to reduce redundancy.

        :param file_path: Path object pointing to location of file with node information
        :return:
        """

        # Create skeleton indexing scheme
        indexed_nodes = {}

        # Index nodes
        for node in from_json(file_path, compressed=self.compressed):

            properties = node['node_props']
            properties[self.unique_prop_key] = node['node_id']

            node_labels = node['node_labels']

            if node_labels in indexed_nodes.keys():
                indexed_nodes[node['node_labels']].append(properties)
            else:
                indexed_nodes[node_labels] = [properties]

        with self.driver.session(database=self.database) as session:

            for label, rows in indexed_nodes.items():
                query = f"""

                    UNWIND $rows as row
                        CREATE (a:{label})
                        SET a += row

                """

                session.run(query, rows=rows)

    def _import_relationships_file(self, file_path):

        """
        This function runs into a lot of the same problems as the self._import_nodes_file(), in that
        node labels can not be dynamically allocated in a Neo4j query. But, relationship types can not be
        passed either. So, three nested for loops are needed to go through the combination of labels and relationship
        types.

        :param file_path:
        :return:
        """

        # Create skeleton indexing
        indexed_relationships = []
        labels = set()
        rel_types = set()

        # Index relationships
        for relationship in from_json(file_path, compressed=self.compressed):
            row = relationship
            labels.add(row['start_node_labels'])
            labels.add(row['end_node_labels'])
            rel_types.add(row['rel_type'])
            indexed_relationships.append(row)

        with self.driver.session(database=self.database) as session:

            for start_label in labels:
                for end_label in labels:
                    for rel_type in rel_types:

                        query = f"""
            
                        UNWIND $rows as row
                            MATCH (start_node:{start_label})
                                WHERE start_node.{self.unique_prop_key} = row.start_node_id
                            MATCH (end_node:{end_label})
                                WHERE end_node.{self.unique_prop_key} = row.end_node_id
                            CREATE (start_node)-[r:{rel_type}]->(end_node)
                                SET r += row.rel_props
                            """

                        session.run(query, rows=indexed_relationships)

    def _cleanup(self):

        with self.driver.session(database=self.database) as session:

            # Drop dummy unique property key used for merging nodes
            query = f"""

            MATCH (a) WHERE EXISTS(a.{self.unique_prop_key})
            REMOVE a.{self.unique_prop_key}

            """
            session.run(query)

            # Drop dummy constraints used to speed up merging nodes
            for node_labels in tqdm(self.labels, desc='Removing Temporary Constraints'):
                node_labels = node_labels.split(":")
                for node_label in node_labels:
                    constraint = f" DROP CONSTRAINT {node_label}_{self.unique_prop_key} IF EXISTS"
                    session.run(constraint)

            # Add original constraints
            # Note it is slower to add these earlier
