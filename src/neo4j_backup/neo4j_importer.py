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

        :param project_dir: The directory where to back up Neo4j Graph
        :param driver: Neo4j driver
        :param input_yes: bool, determines weather to just type in "y" for all input options
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
        self.uniqueness_constraints: list = from_json(self.project_dir / 'uniqueness_constraints.json')

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
        This is the main function in this class, this actually imports the data into Neo4j.

        :return:
        """

        self._test_connection()  # Make sure the driver can connect to Neo4j database
        self._verify_is_new_db()  # Make sure database is empty and not the original database

        self._apply_temp_constraints()  # Apply the dummy constraints

        # Grab all the labels used for Nodes
        for file_path in tqdm(self.nodes_files, desc="Inserting Nodes"):
            self._import_nodes_file(file_path)

        # Grab all the relationship types used by relationships
        for file_path in tqdm(self.relationships_files, desc="Inserting Relationships"):
            self._import_relationships_file(file_path)

        # Remove dummy constraints and properties, add real constraints, fix temporal/spatial values
        self._fix_node_temporal_spatial_values()
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

    @staticmethod
    def _apply_constraint(session, constraint):
        try:
            constraint_str = f"""
            CREATE CONSTRAINT {constraint['constraint_name']} 
            FOR (n:{constraint['node_label']}) 
            REQUIRE n.{constraint['node_prop']} IS UNIQUE
            """
            session.run(constraint_str)
            return
        except:
            constraint_str = f"""
            CREATE CONSTRAINT {constraint['constraint_name']} 
            ON (n:{constraint['node_label']}) 
            ASSERT n.{constraint['node_prop']} IS UNIQUE
            """
            session.run(constraint_str)

    def _apply_temp_constraints(self):

        with self.driver.session(database=self.database) as session:

            # Create dummy constraints on each node label, helps speed up inserting significantly
            for node_label in tqdm(self.labels, desc='Applying Temporary Constraints'):
                if ":" not in node_label:
                    constraint = dict(
                        node_label=node_label,
                        node_prop=self.unique_prop_key,
                        constraint_name=f"{node_label}_{self.unique_prop_key}",
                    )
                    self._apply_constraint(session, constraint)

    def _import_nodes_file(self, file_path):

        """
        Import nodes from node files

        :param file_path: Path object pointing to location of file with node information
        :return:
        """

        with self.driver.session(database=self.database) as session:

            data = from_json(file_path, compressed=self.compressed)

            for node_labels in self.labels:

                filtered_data = []
                for row in data:
                    if row["node_labels"] == node_labels:
                        filtered_data.append(row)

                query = f"""
                    UNWIND $rows as row
                    CREATE (a:{node_labels})
                    SET a.{self.unique_prop_key} = row["node_id"]
                    SET a += row["node_props"]
                """

                session.run(query, parameters={"rows": filtered_data})

    def _import_relationships_file(self, file_path):

        """
        Import relationships from relationship files

        :param file_path:
        :return:
        """

        with self.driver.session(database=self.database) as session:

            data = from_json(file_path, compressed=self.compressed)

            for relationship in self.relationship_types:

                filtered_data = []
                for row in data:
                    if row["rel_type"] == relationship:
                        filtered_data.append(row)

                query = f"""
                UNWIND $rows as row
                MATCH (start_node)
                    WHERE start_node.{self.unique_prop_key} = row["start_node_id"]
                MATCH (end_node)
                    WHERE end_node.{self.unique_prop_key} = row["end_node_id"]
                CREATE (start_node)-[r:{relationship}]->(end_node)
                SET r.{self.unique_prop_key} = row["rel_id"]
                SET r += row["rel_props"]
                """

                session.run(query, parameters={"rows": filtered_data})

    def _fix_node_temporal_spatial_values(self):

        literals = ["point(", "date(", "time(", "datetime(", "duration("]

        with self.driver.session(database=self.database) as session:

            # Fix nodes
            query = f"""
            MATCH (node)
            RETURN (node)
            """

            # Gather number of nodes
            number_of_nodes = session.run("MATCH (node) RETURN COUNT(node)").value()[0]
            results = session.run(query)

            # Going through all properties in all nodes
            for record in tqdm(results, total=number_of_nodes, desc="Extracting Nodes"):
                node = record['node']
                node_props = dict(node)
                for prop_key, prop_value in node_props.items():
                    if isinstance(prop_value, str):
                        for literal in literals:
                            if prop_value[:len(literal)] == literal:

                                # If property is a spatial or temporal value, update the property
                                query = f"""
                                MATCH (n)
                                WHERE n.{self.unique_prop_key} = {node_props[self.unique_prop_key]}
                                SET n.{prop_key} = {prop_value}
                                RETURN n
                                """
                                session.run(query)

                            elif prop_value[:len(literal) + 1] == "$" + literal:

                                # If property is a literal string with a spartial/temporal piece
                                # Remove the literal identifier
                                prop_value = prop_value[1:]
                                query = f"""
                                MATCH (n)
                                WHERE n.{self.unique_prop_key} = {node_props[self.unique_prop_key]}
                                SET n.{prop_key} = "{prop_value}"
                                """
                                session.run(query)

    def _cleanup(self):

        with self.driver.session(database=self.database) as session:

            # Drop dummy unique property key used for merging nodes
            query = f"""
            MATCH (a) WHERE a.{self.unique_prop_key} IS NOT NULL
            REMOVE a.{self.unique_prop_key}
            """
            session.run(query)

            # Drop dummy unique property key used for match relationships in cleanup
            query = f"""
            MATCH ()-[r]-() WHERE r.{self.unique_prop_key} IS NOT NULL
            REMOVE r.{self.unique_prop_key}
            """
            session.run(query)

            # Drop dummy constraints used to speed up merging nodes
            for node_labels in tqdm(self.labels, desc='Removing Temporary Constraints'):
                if ":" not in node_labels:
                    constraint = f"DROP CONSTRAINT {node_labels}_{self.unique_prop_key}"
                    session.run(constraint)

            # Apply real constraints
            if self.uniqueness_constraints:
                for constraint in tqdm(self.uniqueness_constraints, desc='Applying Actual Constraints',
                                       total=len(self.uniqueness_constraints)):
                    self._apply_constraint(session, constraint)
