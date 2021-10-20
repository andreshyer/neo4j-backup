# neo4j-backup
This is a project that is designed at downloading and uploading data in Neo4j Knowledge Graphs without Neo4j 
Enterprise edition. This is useful if dump files cannot be used, or if you want to download the data that exist in
a Neo4j database in a human-readable format.

# Overview

This repo is not intended to replace the naive Neo4j backup dump files,
but rather to be used in instances where a dump file is not an option.
Such as moving data from Neo4j to a different type of database that is not Neo4j, 
storing backups in different formats encase something happens to the original dump file backup,
or having to downgrade a Neo4j graph.

Also, this repo aims to be as simplistic as possible with two main purposes. 
To download a Neo4j graph without using a dump file and to be able to upload that data to a different Neo4j graph.
Only simple cypher statements are used to import and extract data from Neo4j.
The data is downloaded as json files.
The json files are compressed with gzip protocol by default,
but you can choose to export the data without compressing.

When creating this tool, Enterprise tools were not used. 
Meaning that APOC or any other Enterprise/Desktop exclusive tool is not needed, 
and this can be used on the community edition of Neo4j. 

This repo differs from most other Neo4j backup repos in that the Neo4j graph does not need to be a specific instance. 
This code will work with a Neo4j database that is running in Aura, docker, desktop, command-line, server, etc. 
The only requirements are that the python neo4j-driver needs to be able to connect to the database,
that your user has read privileges for downloading data, and write privileges for importing data.

# Packages required

`python: >= 3.5`

`neo4j: >= 4.3.0`

`tqdm: >= 4.10.0`

# Installation

`pip install neo4j-backup`

# Usage

The exact parameters that should be used to access the database depends on the version of the Neo4j graph that you
are trying to access. The python neo4j-driver documentation can be found at 
https://neo4j.com/docs/api/python-driver/current/api.html.

There will be times when the script ask the user for input for (y/N) questions, 
you can set `input_yes=True` to enter yes to all input questions.

## Extracting

```python
from neo4j import GraphDatabase
from neo4j_backup import Extractor

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
    compress = True
    extractor = Extractor(project_dir="data_dump", driver=driver, database=database, input_yes=input_yes, compress=compress)
    extractor.extract_data()
```

## Importing

```python
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
    importer = Importer(project_dir="data_dump", driver=driver, database=database, input_yes=input_yes)
    importer.import_data()
```

# Notes

This may not be the best tool too backup data if speed is a concern.
This tool is significantly slower than the built-in dump tool neo4j provides.
The selling point of this script is also its biggest downfall, all calls to Neo4j are done with cypher.
This adds a significant amount of overhead that can be avoided if the direct files of a graph can be accessed.
Also, while the raw data is machine-readable, 
it still needs to be manipulated by the end to insert it into other databases.

Another note, an internal ID property is made when creating Nodes and properties. 
Since this script does not read the underlying file in the Neo4j database, 
some unique identifier is needed to MATCH nodes on.
Forcing the user to pass a map of unique keys for each NODE is not reasonable.
This temporary internal ID property is removed from each Node at the very end.
The Neo4j database still stores that this property existed at some point on a Node,
so the property will show up on the left side of the Neo4j Browser and when running "CALL db.propertyKeys()".
The temporary property key can not be removed from the list of internal property keys, and the issue is reported at
https://github.com/neo4j/neo4j/issues/10941.

If you need to extract data from a database with this tool, 
and are importing to a database where you have access to the neo4j-admin console.
One option is to use the Importer from neo4j_import to import the initial data into a database,
then dump that database and restore it to a new database. 
Or, you can use a tool like store-utils https://github.com/jexp/store-utils.

This really is not so much an issue, more so as an inconvenience.
If you are using the Neo4j Browser,
it is highly recommended to just use the built-in dump tool.
