from neo4j_backup import Importer

if __name__ == "__main__":
    uri = "neo4j://localhost:7687"
    database = "neo4j"
    username = "neo4j"
    password = "password"
    encrypted = False
    trust = "TRUST_ALL_CERTIFICATES"

    project_dir = "data_dump"

    importer = Importer(project_dir="data_dump", uri=uri, database=database, username=username, password=password,
                        encrypted=encrypted, trust=trust)
    importer.import_data()
