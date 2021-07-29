# neo4j-backup
This is a project that is designed at downloading and uploading data in Neo4j Knowledge Graphs without Neo4j 
Enterprise edition. This is useful if dump files cannot be used, or if you want to download the data that exist in
a Neo4j database in a human-readable format.

# Overview

Using this repo to backup Neo4j data will not be faster than the built-in dump file that Neo4j provides. 
But, there are some instances where using a tool like this is useful. 
Such as moving data from Neo4j to a different type of database that is not Neo4j, 
or storing backups in different formats encase something happens to the original dump file backup.

Also, this repo aims to be as simplistic as possible with two main purposes. 
To download a Neo4j graph without using dumps file and to be able to upload that data to a different Neo4j graph.
Only simple cypher statements are used to import and extract data from Neo4j.
The data is downloaded as a json files and compressed with gzip compression format.

When creating this tool, Enterprise tools were not used. 
Meaning that APOC or any other Enterprise/Desktop exclusive tool is not needed, 
and this can be used on the community edition of Neo4j. 

This repo differs from most other Neo4j backup repos in that the Neo4j graph does not need to be a specific instance. 
This code will work with a Neo4j database that is running in Aura, docker, desktop, command-line, server, etc. The only
requirements are that the python neo4j-driver need to be able to connect to the database and that your user has read privileges.

# Packages required

`python: >= 3.5`

`neo4j: >= 4.3.0`

`tqdm: >= 4.10.0` : tqdm will be optional in the future 

# Installation

This is coming eventually, but will likely look like

`pip install neo4j-backup`

For now, this repo can be downloaded and used from source

# Usage

The exact parameters that should be used to access the database depends on the version of the Neo4j graph that you
are trying to access. The python neo4j-driver documentation can be found at https://neo4j.com/docs/api/python-driver/current/api.html.

## Extracting

```python
from neo4j_backup import Extractor

if __name__ == "__main__":
    uri = "neo4j://localhost:7687"
    database = "neo4j"
    username = "neo4j"
    password = "password"
    encrypted = False
    trust = "TRUST_ALL_CERTIFICATES"

    project_dir = "data_dump"

    extractor = Extractor(project_dir="data_dump", uri=uri, database=database, username=username, password=password,
                          encrypted=encrypted, trust=trust)
    extractor.extract_data()
```

## Importing

```python
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
```
