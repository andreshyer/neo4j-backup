from neo4j import GraphDatabase
from neo4j_backup import Importer

if __name__ == "__main__":

    uri = "neo4j://localhost:7687"
    username = "neo4j"
    password = "password"
    encrypted = False
    trust = "TRUST_ALL_CERTIFICATES"
    driver = GraphDatabase.driver(uri, auth=(username, password), encrypted=encrypted, trust=trust)

    database = "neo4j"

    project_dir = "data_dump"
    input_yes = False
    importer = Importer(project_dir=project_dir, driver=driver, database=database, input_yes=input_yes)
    importer.import_data()
