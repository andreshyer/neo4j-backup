from tqdm import tqdm
from pathlib import Path
from os import getcwd, listdir

from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable

from ._backends import decompress_json


class Importer:

    def __init__(self, project_dir, uri, database, username, password, encrypted, trust):

        """
        This purpose of this class is to import the information in the project_dir output from the Extractor class.

        :param project_dir: The directory where the Neo4j graph was backed up with using neo4j_backup.Extractor
        :param uri: URI used to access to Neo4j DBMS
        :param database: Database to access inside DBMS, default is Neo4j
        :param username: Neo4j username
        :param password: Neo4j password
        :param encrypted: Weather connection is encrypted, see Neo4j docs
        :param trust: What certificates to trust, see Neo4j docs
        """

        self.uri: str = uri
        self.database: str = database
        self.username: str = username
        self.password: str = password
        self.encrypted: bool = encrypted
        self.trust: bool = trust
        self.driver: GraphDatabase.driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password),
                                                                 encrypted=self.encrypted, trust=trust)
        self.project_dir = Path(getcwd()) / project_dir

        self.unique_prop_key: str = decompress_json(self.project_dir / f"unique_prop_key.json.gz")

        self.relationship_files = []
        for file_path in listdir(self.project_dir):
            if "relationship" in file_path:
                file_path = self.project_dir / file_path
                self.relationship_files.append(file_path)

        self.node_files = []
        for file_path in listdir(self.project_dir):
            if "node" in file_path:
                file_path = self.project_dir / file_path
                self.node_files.append(file_path)

    def import_data(self):

        """
        This is the main function in this class, this actual imports the data import Neo4j.

        :return:
        """

        self._test_connection()  # Make sure the driver can connect to Neo4j database
        self._verify_is_new_db()  # Make sure database is empty and not the original database
        self._apply_constraints()  # Apply the dummy constraints

        # Grab all the labels used for Nodes
        labels = decompress_json(self.project_dir / 'labels.json.gz')
        for file_path in tqdm(self.node_files, desc="Importing Nodes"):
            self._import_node_file(file_path, labels)

        # Grab all the relationship types used by relationships
        relationship_types = decompress_json(self.project_dir / 'types.json.gz')
        for i, file_path in enumerate(self.relationship_files):
            self._import_relationship_file(i, file_path, relationship_types, labels)

        # Remove dummy constraints and properties
        self._cleanup(labels)

    def _test_connection(self):
        try:
            with self.driver.session(database=self.database) as session:
                session.run("MATCH (a) RETURN a LIMIT 1")
        except ServiceUnavailable:
            raise ServiceUnavailable("Unable to connect to database. If this is a local database, make sure the "
                                     "database is running. If this is a remote database, make sure the correct "
                                     "database is referenced.")

    def _verify_is_new_db(self):

        old_db_is = decompress_json(file_path=self.project_dir / f"db_id.json.gz")

        # Get current working database id
        with self.driver.session(database=self.database) as session:
            results = session.run("CALL db.info")
            for result in results:
                current_db_id = dict(result)['id']

        # Grab ping data
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

        # If ping data is not empty, then the database is not empty
        elif data:
            user_input = input("The database referenced is not empty. Proceed anyway? (y/N)\n")
            if user_input != "y":
                raise UserWarning("Aborted")

    def _apply_constraints(self):

        labels = decompress_json(self.project_dir / 'labels.json.gz')
        with self.driver.session(database=self.database) as session:

            # Create dummy constraints on each node label, helps speed up inserting significantly
            for node_labels in labels:
                node_labels = node_labels.split(":")
                for node_label in node_labels:
                    constraint = f"CREATE CONSTRAINT {node_label}_{self.unique_prop_key} IF NOT EXISTS ON " \
                                 f"(m:{node_label}) ASSERT (m.{self.unique_prop_key}) IS UNIQUE"
                    session.run(constraint)

    def _import_node_file(self, file_path, labels):

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

        and trying to merge two separate nodes, one for the Person label and one for the Banker label. Then trying to drop
        the duplicate node and adding the label later. It is easier to look at Person:Banker as a separate label, making
        sure the combination of labels are sorted in alphabetical order to reduce redundancy.

        :param file_path: Path object pointing to location of file with node information
        :param labels: List of labels
        :return:
        """

        nodes = decompress_json(file_path)

        for label in labels:

            # TODO make this query look nicer
            query = f"""
            
                UNWIND $rows as row
                    MERGE (a:{label} {'{'}{self.unique_prop_key}: row.node_id{'}'})
                    ON CREATE SET a += row.properties
                
            """

            # Grab all nodes that have the same label as the working label
            filtered_nodes = []
            for node_id, node in nodes.items():
                node_labels = node['labels']
                if label == node_labels:

                    # Format dict for UNWIND statement
                    filtered_nodes.append({'node_id': node_id, 'properties': node['properties']})

            with self.driver.session(database=self.database) as session:
                session.run(query, rows=filtered_nodes)

    def _import_relationship_file(self, i, file_path, relationship_types, labels):

        """
        This function runs into a lot of the same problems as the self._import_node_file(), in that
        node labels can not be dynamically allocated in a Neo4j query. But, relationship types can not be
        passed either. So, three nested for loops are needed to go through the combination of labels and relationship
        types.

        :param i:
        :param file_path:
        :param relationship_types:
        :param labels:
        :return:
        """

        relationships = decompress_json(file_path)

        for relationship_type in tqdm(relationship_types,
                                      desc=f"Importing Relationships in file ({i}/{len(self.relationship_files)})"):
            for label_1 in labels:
                for label_2 in labels:

                    # TODO make this query look nicer
                    query = f"""

                    UNWIND $rows as row
                        MERGE (start_node:{label_1} {'{'}{self.unique_prop_key}: row.start_node{'}'})
                        MERGE (end_node:{label_2} {'{'}{self.unique_prop_key}: row.end_node{'}'})
                        MERGE (start_node)-[r:{relationship_type}]->(end_node)
                        ON CREATE SET r += row.properties

                    """

                    filter_relationships = []
                    for relationship in relationships:

                        # Grab relationship where node labels match
                        if relationship['start_node_labels'] == label_1 and relationship['end_node_labels'] == label_2:

                            # And where relationship type match the working parameters above in for loops
                            if relationship_type == relationship['relationship_type']:

                                # Format dict for UNWIND
                                filter_relationships.append({'start_node': relationship['start_node'],
                                                             'end_node': relationship['end_node'],
                                                             'properties': relationship['relationship_properties']})

                    with self.driver.session(database=self.database) as session:
                        session.run(query, rows=filter_relationships)

    def _cleanup(self, labels):

        with self.driver.session(database=self.database) as session:

            # Drop dummy unique property key used for merging nodes
            query = f"""

            MATCH (a) WHERE EXISTS(a.{self.unique_prop_key})
            REMOVE a.{self.unique_prop_key}

            """
            session.run(query)

            # Drop dummy constraints used to speed up merging nodes
            for node_labels in labels:
                node_labels = node_labels.split(":")
                for node_label in node_labels:
                    constraint = f" DROP CONSTRAINT {node_label}_{self.unique_prop_key} IF EXISTS"
                    session.run(constraint)

            # Add original constraints
            constraints = decompress_json(self.project_dir / 'constraints.json.gz')
            for constraint in constraints:
                constraint = constraint.split("CONSTRAINT")[1]
                constraint = "CREATE CONSTRAINT IF NOT EXISTS" + constraint
                session.run(constraint)
