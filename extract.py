from shutil import rmtree
from pathlib import Path
from sys import getsizeof
from os.path import exists
from os import mkdir, getcwd

from tqdm import tqdm
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable

from backends import compress_json, number_to_letters, gather_labels, decompress_json, get_unique_prop_key


class Extractor:

    def __init__(self, project_dir, port, database, username, password, encrypted):
        self.port: str = port
        self.database: str = database
        self.username: str = username
        self.password: str = password
        self.encrypted: bool = encrypted
        self.driver: GraphDatabase.driver = GraphDatabase.driver(self.port, auth=(self.username, self.password),
                                                                 encrypted=self.encrypted)
        self.project_dir: Path = Path(getcwd()) / project_dir

    def extract_data(self, max_ram=2, units="MB"):

        self._test_connection()
        self._verify_df_not_empty()

        units_dict = {"KB": 10 ** 3, "MB": 10 ** 6}
        unit_number = units_dict[units]
        max_ram = max_ram * unit_number

        if exists(self.project_dir):
            user_input = input(f"The directory {self.project_dir} already exist, would you like to replace the "
                               f"directory? (y/N)\n")
            if user_input.lower() == "y":
                rmtree(self.project_dir)
            else:
                raise UserWarning("Aborted")
        mkdir(self.project_dir)

        self._pull_db_id()
        self._pull_constraints()
        self._pull_nodes(max_ram=max_ram)
        self._pull_relationships(max_ram=max_ram)

        unique_prop_key = self._calc_unique_prop_key()
        compress_json(file_path=self.project_dir / f"unique_prop_key.json", data=unique_prop_key)

    def _test_connection(self):
        try:
            with self.driver.session(database=self.database) as session:
                session.run("MATCH (a) RETURN a LIMIT 1")
        except:
            raise ServiceUnavailable("Unable to connect to database. If this is a local database, make sure the "
                                     "database is running. If this is a remote database, make sure the correct "
                                     "database is referenced.")

    def _verify_df_not_empty(self):
        with self.driver.session(database=self.database) as session:
            results = session.run("MATCH (a) RETURN a LIMIT 1").data()
            if not results:
                raise LookupError("There is not data to pull from the database, make sure the correct database is "
                                  "referenced/running.")

    def _pull_db_id(self):

        with self.driver.session(database=self.database) as session:
            results = session.run("CALL db.info")
            for result in results:
                db_id = dict(result)['id']
        compress_json(file_path=self.project_dir / f"db_id.json", data=db_id)

    def _pull_constraints(self):

        constraints = []
        with self.driver.session(database=self.database) as session:
            results = session.run("CALL db.constraints")
            for result in results:
                constraints.append(dict(result)['description'])

        compress_json(file_path=self.project_dir / f"constraints.json", data=constraints)

    def _pull_nodes(self, max_ram):

        query = """
        
        MATCH (a)
        RETURN id(a) as ID, a as Node 
        
        """

        extracted_data = {}
        properties = set()
        labels = set()

        with self.driver.session(database=self.database) as session:
            number_of_nodes = session.run("MATCH (a) RETURN COUNT(a)").value()[0]

            results = session.run(query)
            for index, record in enumerate(tqdm(results, total=number_of_nodes, desc="Extracting Nodes")):
                node_id = number_to_letters(record['ID'])
                node = record['Node']

                node_labels = list(node.labels)
                node_labels.sort()
                node_labels = ":".join(node_labels)
                labels.add(node_labels)

                node_properties = dict(node)
                properties.update(node_properties.keys())

                extracted_data[node_id] = {'labels': node_labels, 'properties': node_properties}

                if index % 1000 == 0 and index != 0:
                    size_in_ram = getsizeof(extracted_data)
                    if size_in_ram > max_ram:
                        compress_json(file_path=self.project_dir / f"nodes_{index}.json", data=extracted_data)
                        extracted_data = []

            compress_json(file_path=self.project_dir / f"nodes_{index}.json", data=extracted_data)

            properties = list(properties)
            compress_json(file_path=self.project_dir / "properties.json", data=properties)

            labels = list(labels)
            compress_json(file_path=self.project_dir / "labels.json", data=labels)

    def _pull_relationships(self, max_ram):

        query = """

        MATCH ()-[r]-()
        RETURN r, TYPE(r) as type

        """

        extracted_data = []
        types = set()

        with self.driver.session(database=self.database) as session:
            number_of_relationships = session.run("MATCH ()-[r]-() RETURN COUNT(r)").value()[0]
            results = session.run(query)

            for index, record in enumerate(tqdm(results, total=number_of_relationships,
                                                desc="Extracting Relationships")):
                relationship_type = record['type']
                types.add(relationship_type)

                relationship = record['r']
                relationship_properties = dict(relationship)

                start_node_id = number_to_letters(relationship.start_node.id)
                end_node_id = number_to_letters(relationship.end_node.id)

                extracted_data.append({"start_node": start_node_id, "start_node_labels": [],
                                       "end_node": end_node_id, "end_node_labels": [],
                                       "relationship_type": relationship_type,
                                       "relationship_properties": relationship_properties})

                if index % 1000 == 0 and index != 0:
                    size_in_ram = getsizeof(extracted_data)
                    if size_in_ram > max_ram:
                        extracted_data = gather_labels(self.project_dir, extracted_data)
                        compress_json(file_path=self.project_dir / f"relationship_{index}.json", data=extracted_data)
                        extracted_data = []
            extracted_data = gather_labels(self.project_dir, extracted_data)
            compress_json(file_path=self.project_dir / f"relationship_{index}.json", data=extracted_data)

            types = list(types)
            compress_json(file_path=self.project_dir / "types.json", data=types)

    def _calc_unique_prop_key(self):
        file_path = self.project_dir / 'properties.json.gz'
        node_properties = decompress_json(file_path)

        # Neo4j's built in IDs can change as new entities are added. So, a unique property is generated where the
        # pulled ids are placed temporarily. A unique property is calculated because we do not want to 'create' a
        # dummy property that the user actually uses.
        unique_prop_key = get_unique_prop_key(node_properties)
        return unique_prop_key


if __name__ == "__main__":
    port = "neo4j://localhost:7687"
    database = "neo4j"
    username = "neo4j"
    password = "password"
    encrypted = False

    project_dir = "data_dump"

    extractor = Extractor("data_dump", port, database, username, password, encrypted)
    extractor.extract_data()

    # with GraphDatabase.driver(port, auth=(username, password), database=database, encrypted=True) as driver:
    #     print("Pass")
