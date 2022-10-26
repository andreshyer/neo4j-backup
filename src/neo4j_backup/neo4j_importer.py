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
        This is the main function in this class, this actually imports the data into Neo4j.

        :return:
        """

        self._test_connection()  # Make sure the driver can connect to Neo4j database
        self._verify_is_new_db()  # Make sure database is empty and not the original database
        self._apply_constraints()  # Apply the dummy constraints

        # Grab all the labels used for Nodes
        for file_path in self.nodes_files:
            self._import_nodes_file(file_path)

        # Grab all the relationship types used by relationships
        for file_path in self.relationships_files:
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
            for node_label in tqdm(self.labels, desc='Applying Temporary Constraints'):
                constraint = f"CREATE CONSTRAINT {node_label}_{self.unique_prop_key} IF NOT EXISTS ON " \
                             f"(m:{node_label}) ASSERT (m.{self.unique_prop_key}) IS UNIQUE"
                session.run(constraint)

    @staticmethod
    def _reformat_props(props, prop_types):

        # Re-format property in a way that Neo4j understands
        def _reformat_prop(prop_type, prop):
            if prop_type == "int" or prop_type == "bool" or prop_type == "float":
                reformatted_prop = prop
            elif prop_type == "str":
                reformatted_prop = prop.replace('"', '\\"')
                reformatted_prop = reformatted_prop.replace("'", "\\'")
                reformatted_prop = f'"{reformatted_prop}"'
            elif prop_type == "2d-cartesian-point":
                reformatted_prop = f"point({'{'}x: {prop[0]}, y: {prop[1]}, crs: 'cartesian'{'}'})"
            elif prop_type == "3d-cartesian-point":
                reformatted_prop = f"point({'{'}x: {prop[0]}, y: {prop[1]}, z: {prop[2]}, crs: 'cartesian-3d'{'}'})"
            elif prop_type == "2d-WGS-84-point":
                reformatted_prop = f"point({'{'}x: {prop[0]}, y: {prop[1]}, crs: 'wgs-84'{'}'})"
            elif prop_type == "3d-WGS-84-point":
                reformatted_prop = f"point({'{'}x: {prop[0]}, y: {prop[1]}, z: {prop[2]}, crs: 'wgs-84-3d'{'}'})"
            elif prop_type == "date":
                reformatted_prop = f'date("{prop}")'
            elif prop_type == "time":
                reformatted_prop = f'time("{prop}")'
            elif prop_type == "datetime":
                reformatted_prop = f'datetime("{prop}")'
            elif prop_type == "duration":
                duration_str = ""
                for time_key, time_value in prop.items():
                    duration_str += f"{time_key}: {time_value},"
                duration_str = duration_str.split(",")[:-1]
                duration_str = ", ".join(duration_str)
                duration_str = "{" + duration_str + "}"
                reformatted_prop = f'duration({duration_str})'
            else:
                raise ValueError(f"Property type {prop_type} is not supported")
            return reformatted_prop

        # Goal is to have a final dict of {prop_key: prop_value_as_str}
        # Where prop_value_as_str is the original prop_value formatted in a way Neo4j understands
        new_props = {}
        for prop_key, prop_type in prop_types.items():
            prop = props[prop_key]

            # Treat property that are list like normal if they are points
            point_types = ["2d-cartesian-point", "3d-cartesian-point", "2d-WGS-84-point", "3d-WGS-84-point"]
            if prop_type in point_types:
                new_props[prop_key] = _reformat_prop(prop_type, prop)

            # Treat each property inside an array separately
            elif isinstance(prop, list):
                array_props = []
                for i, sub_prop in enumerate(prop):
                    sub_prop_type = prop_type[i]
                    array_props.append(_reformat_prop(sub_prop_type, sub_prop))
                array_str = ""
                for array_prop in array_props:
                    array_str += f"{array_prop}, "
                array_str = array_str.split(",")[:-1]
                array_str = ", ".join(array_str)
                array_str = "[" + array_str + "]"
                new_props[prop_key] = array_str

            # Treat all other properties as normal
            else:
                new_props[prop_key] = _reformat_prop(prop_type, prop)

        return new_props

    def _import_nodes_file(self, file_path):

        """
        Import nodes from node files

        :param file_path: Path object pointing to location of file with node information
        :return:
        """

        with self.driver.session(database=self.database) as session:

            # For each node in the given file
            for node in tqdm(from_json(file_path, compressed=self.compressed),
                             desc=f"Inserting nodes from {file_path.stem}"):

                node_id = node["node_id"]
                node_labels = node["node_labels"]
                node_labels = ":".join(node_labels)

                # Gather properties
                node_props = node["node_props"]
                node_prop_types = node["node_props_types"]
                node_props = self._reformat_props(node_props, node_prop_types)

                # Run query, inserting one node at a time
                query = f"""
                    CREATE (a:{node_labels})
                    SET a.{self.unique_prop_key} = {node_id}\n
                """

                # Add all properties associated with a node
                for prop_key, prop_value in node_props.items():
                    query += f"SET a.{prop_key} = {prop_value}\n"

                session.run(query, parameters={"node_props": node_props})

    def _import_relationships_file(self, file_path):

        """
        Import relationships from relationship files

        :param file_path:
        :return:
        """

        with self.driver.session(database=self.database) as session:

            for relationship in tqdm(from_json(file_path, compressed=self.compressed),
                                     desc=f"Inserting relationships from {file_path.stem}"):

                start_node_id = relationship["start_node_id"]
                start_node_labels = relationship["start_node_labels"]
                start_node_labels = ":".join(start_node_labels)

                end_node_id = relationship["end_node_id"]
                end_node_labels = relationship["end_node_labels"]
                end_node_labels = ":".join(end_node_labels)

                rel_type = relationship["rel_type"]
                rel_props = relationship["rel_props"]
                rel_props_types = relationship["rel_props_types"]
                rel_props = self._reformat_props(rel_props, rel_props_types)

                query = f"""
                    MATCH (start_node:{start_node_labels})
                        WHERE start_node.{self.unique_prop_key} = {start_node_id}
                    MATCH (end_node:{end_node_labels})
                        WHERE end_node.{self.unique_prop_key} = {end_node_id}
                    CREATE (start_node)-[r:{rel_type}]->(end_node)\n
                    """

                for prop_key, prop_value in rel_props.items():
                    query += f"SET r.{prop_key} = {prop_value}\n"

                session.run(query, parameters={"rel_props": rel_props})

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

            # Apply real constraints
            for constraint in tqdm(self.constraints, desc='Applying Actual Constraints', total=len(self.constraints)):
                constraint = constraint.split("CONSTRAINT")[1]
                constraint = "CREATE CONSTRAINT IF NOT EXISTS" + constraint
                session.run(constraint)
