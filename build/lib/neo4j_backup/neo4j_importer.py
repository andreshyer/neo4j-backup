from tqdm import tqdm
from pathlib import Path
from os import getcwd, listdir

from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable

from ._backends import from_json, to_json


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
        self.labels: list[str] = from_json(self.project_dir / 'labels.json')
        self.relationship_types: list[str] = from_json(self.project_dir / 'types.json')
        self.constraints: list[str] = from_json(self.project_dir / 'constraints.json')

        self.relationships_files = []
        for file_path in listdir(self.data_dir):
            if "relationship" in file_path:
                file_path = self.data_dir / file_path
                self.relationships_files.append(file_path)

        self.lonely_nodes_files = []
        for file_path in listdir(self.data_dir):
            if "node" in file_path:
                file_path = self.data_dir / file_path
                self.lonely_nodes_files.append(file_path)

    def import_data(self):

        """
        This is the main function in this class, this actual imports the data import Neo4j.

        :return:
        """

        self._test_connection()  # Make sure the driver can connect to Neo4j database
        self._verify_is_new_db()  # Make sure database is empty and not the original database
        self._apply_constraints()  # Apply the dummy constraints

        # Grab all the labels used for Nodes
        for file_path in tqdm(self.lonely_nodes_files, desc="Importing Nodes"):
            self._import_lonely_nodes_file(file_path)

        # Grab all the relationship types used by relationships
        for i, file_path in enumerate(self.relationships_files):
            self._import_relationships_file(i, file_path)

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

    def _import_lonely_nodes_file(self, file_path):

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
        drop the duplicate node and adding the label later. It is easier to look at Person:Banker as a separate label,
        making sure the combination of labels are sorted in alphabetical order to reduce redundancy.

        :param file_path: Path object pointing to location of file with node information
        :return:
        """

        # Create skeleton indexing scheme
        indexed_nodes = {}

        # Index nodes
        for node in from_json(file_path, compressed=self.compressed):

            row = {'node_id': node['node_id'], 'properties': node['node_props']}

            node_labels = node['node_labels']

            if node_labels in indexed_nodes.keys():
                indexed_nodes[node['node_labels']].append(row)
            else:
                indexed_nodes[node_labels] = [row]

        with self.driver.session(database=self.database) as session:

            for label, rows in indexed_nodes.items():

                query = f"""

                    UNWIND $rows as row
                        CREATE (a:{label} {'{'}{self.unique_prop_key}: row.node_id{'}'})
                            SET a += row.properties

                """

                session.run(query, rows=rows)

    def _import_relationships_file(self, i, file_path):

        """
        This function runs into a lot of the same problems as the self._import_lonely_nodes_file(), in that
        node labels can not be dynamically allocated in a Neo4j query. But, relationship types can not be
        passed either. So, three nested for loops are needed to go through the combination of labels and relationship
        types.

        :param i:
        :param file_path:
        :return:
        """


        # Create skeleton indexing
        indexed_relationships = {}

        # Index relationships
        for relationship in from_json(file_path, compressed=self.compressed):

            row = {'start_node_id': relationship['start_node_id'],
                    'start_node_props': relationship['start_node_props'],
                    'end_node_id': relationship['end_node_id'],
                    'end_node_props': relationship['end_node_props'],
                    'properties': relationship['rel_props'],
                    'raw_rel': relationship}

            rel_type = relationship['rel_type']
            start_labels = relationship['start_node_labels']
            end_labels = relationship['end_node_labels']
            index_str = f"{rel_type}||{start_labels}||{end_labels}"

            if index_str in indexed_relationships.keys():
                indexed_relationships[index_str].append(row)
            else:
                indexed_relationships[index_str] = [row]

        with self.driver.session(database=self.database) as session:

            for index_str, rows in tqdm(indexed_relationships.items(),
                                    desc=f"Importing Relationships in file ({i+1}/{len(self.relationships_files)})"):

                index_str = index_str.split("||")
                rel_type = index_str[0]
                label_1 = index_str[1]
                label_2 = index_str[2]

                query = f"""

                UNWIND $rows as row
                    MERGE (start_node:{label_1} {'{'}{self.unique_prop_key}: row.start_node_id{'}'})
                        ON CREATE SET start_node += row.start_node_props
                    MERGE (end_node:{label_2} {'{'}{self.unique_prop_key}: row.end_node_id{'}'})
                        ON CREATE SET end_node += row.end_node_props
                    CREATE (start_node)-[r:{rel_type}]->(end_node)
                        SET r += row.properties

                """

                session.run(query, rows=rows)

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
            for constraint in tqdm(self.constraints, desc='Applying Actual Constraints'):
                constraint = constraint.split("CONSTRAINT")[1]
                constraint = "CREATE CONSTRAINT IF NOT EXISTS" + constraint
                session.run(constraint)
