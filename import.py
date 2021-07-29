from tqdm import tqdm
from pathlib import Path
from os import getcwd, listdir

from neo4j import GraphDatabase

from backends import decompress_json, format_props


class Importer:

    def __init__(self, project_dir, port, database, username, password, encrypted):
        self.port: str = port
        self.database: str = database
        self.username: str = username
        self.password: str = password
        self.encrypted: bool = encrypted
        self.driver: GraphDatabase.driver = GraphDatabase.driver(self.port, auth=(self.username, self.password),
                                                                 encrypted=self.encrypted)
        self.project_dir: Path = Path(getcwd()) / project_dir

        self.unique_prop_key: str = decompress_json(self.project_dir / f"unique_prop_key.json.gz")

        self.relationship_files: list[Path] = []
        for file_path in listdir(self.project_dir):
            if "relationship" in file_path:
                file_path = self.project_dir / file_path
                self.relationship_files.append(file_path)

        self.node_files: list[Path] = []
        for file_path in listdir(self.project_dir):
            if "node" in file_path:
                file_path = self.project_dir / file_path
                self.node_files.append(file_path)

    def import_data(self):

        self._test_connection()
        self._verify_is_new_db()
        self._apply_constraints()

        file_path = self.project_dir / 'labels.json.gz'
        labels = decompress_json(file_path)
        for file_path in tqdm(self.node_files, desc="Importing Nodes"):
            self._import_node_file(file_path, labels)

        file_path = self.project_dir / 'types.json.gz'
        relationship_types = decompress_json(file_path)
        for file_path in tqdm(self.relationship_files, desc="Importing Relationships"):
            self._import_relationship_file(file_path, relationship_types, labels)

        self._cleanup(labels)

    def _test_connection(self):
        with self.driver.session(database=self.database) as session:
            session.run("MATCH (a) RETURN a LIMIT 1")

    def _verify_is_new_db(self):

        old_db_is = decompress_json(file_path=self.project_dir / f"db_id.json.gz")

        with self.driver.session(database=self.database) as session:
            results = session.run("CALL db.info")
            for result in results:
                current_db_id = dict(result)['id']

        with self.driver.session(database=self.database) as session:
            data = session.run("MATCH (a) RETURN a LIMIT 1").data()

        if old_db_is == current_db_id:
            user_input = input("The database currently referenced is the same database that the data was extracted "
                               "from. "
                               "\nIt is highly recommended to abort and make sure that the correct database is "
                               "selected, "
                               "\nas ignoring the warning will likely results in duplicate nodes. "
                               "\nOnly proceed if you really know what you are doing. Would you like to Abort? (y/N)\n")
            if user_input == "y":
                raise UserWarning("Aborted")

        elif data:
            user_input = input("The database referenced is not empty. Proceed anyway? (y/N)\n")
            if user_input != "y":
                raise UserWarning("Aborted")

    def _apply_constraints(self):
        file_path = self.project_dir / 'constraints.json.gz'
        constraints = decompress_json(file_path)

        file_path = self.project_dir / 'labels.json.gz'
        labels = decompress_json(file_path)

        with self.driver.session(database=self.database) as session:
            # for constraint in constraints:
            #     constraint = constraint.split("CONSTRAINT")[1]
            #     constraint = "CREATE CONSTRAINT IF NOT EXISTS" + constraint
            #     session.run(constraint)

            for node_labels in labels:
                node_labels = node_labels.split(":")
                for node_label in node_labels:
                    constraint = f"CREATE CONSTRAINT {node_label}_{self.unique_prop_key} IF NOT EXISTS ON " \
                                 f"(m:{node_label}) ASSERT (m.{self.unique_prop_key}) IS UNIQUE"
                    session.run(constraint)

    def _import_node_file(self, file_path, labels):

        nodes = decompress_json(file_path)

        for label in labels:
            query = f"""
            
                UNWIND $rows as row
                    MERGE (a:{label} {'{'}{self.unique_prop_key}: row.node_id{'}'})
                    ON CREATE SET a += row.properties
                
            """

            filtered_nodes = []
            for node_id, node in nodes.items():
                node_labels = node['labels']
                if label in node_labels:
                    filtered_nodes.append({'node_id': node_id, 'properties': node['properties']})

            with self.driver.session(database=self.database) as session:
                session.run(query, rows=filtered_nodes)

    def _import_relationship_file(self, file_path, relationship_types, labels):

        relationships = decompress_json(file_path)

        for relationship_type in relationship_types:
            for label_1 in labels:
                for label_2 in labels:

                    query = f"""

                    UNWIND $rows as row
                        MERGE (start_node:{label_1} {'{'}{self.unique_prop_key}: row.start_node{'}'})
                        MERGE (end_node:{label_2} {'{'}{self.unique_prop_key}: row.end_node{'}'})
                        MERGE (start_node)-[r:{relationship_type}]->(end_node)
                        ON CREATE SET r += row.properties

                    """

                    filter_relationships = []
                    for relationship in relationships:

                        if relationship['start_node_labels'] == label_1 and relationship['end_node_labels'] == label_2:

                            if relationship_type in relationship['relationship_type']:
                                filter_relationships.append({'start_node': relationship['start_node'],
                                                             'end_node': relationship['end_node'],
                                                             'properties': relationship['relationship_properties']})

                    with self.driver.session(database=self.database) as session:
                        session.run(query, rows=filter_relationships)

    def _cleanup(self, labels):

        # Drop dummy unique property key used for merging nodes
        query = f"""
        
        MATCH (a) WHERE EXISTS(a.{self.unique_prop_key})
        REMOVE a.{self.unique_prop_key}
        
        """

        with self.driver.session(database=self.database) as session:
            session.run(query)

        # Drop dummy constraints used to speed up merging nodes
        for node_labels in labels:
            node_labels = node_labels.split(":")
            for node_label in node_labels:
                constraint = f" DROP CONSTRAINT {node_label}_{self.unique_prop_key} IF EXISTS"
                session.run(constraint)


if __name__ == "__main__":
    port = "neo4j://localhost:7687"
    database = "neo4j"
    username = "neo4j"
    password = "password"
    encrypted = False

    project_dir = "data_dump"

    extractor = Importer("data_dump", port, database, username, password, encrypted)
    extractor.import_data()
