# neo4j-backup
This is a project that is designed at downloading and uploading data in Neo4j Knowledge Graphs without Neo4j 
Enterprise edition. This is useful if Dump files cannot be used, or if you want to download the data that exist in
a Neo4j database in a human-readable format.

# Overview

This repo is not intended to replace the native Neo4j backup Dump files,
but rather to be used in instances where a Dump file is not an option.
Such as moving data from Neo4j to a different type of database.

Also, this repository aims to be as simple as possible with two main purposes. 
To download a Neo4j graph without using a Dump file and to be able to upload that data to a different Neo4j graph.
Only simple Cypher statements are used to import and extract data from Neo4j.
The data is downloaded as json files.
The json files are compressed with the gzip protocol by default,
but you can choose to export the data without compressing.

When creating this tool, Enterprise tools were not used. 
Meaning that APOC or any other Enterprise/Desktop exclusive tool is not needed, 
and this can be used on the community edition of Neo4j. 

This repo differs from most other Neo4j backup repos. 
For this tool, the Neo4j graph does not need to be a specific instance. 
This code will work with a Neo4j database that is running in Aura, docker, desktop, command-line, server, etc. 
The only requirements are that the python neo4j-driver needs to be able to connect to the database,
that your user has read and show constraints privileges for downloading data, and write privileges for importing data.

# Packages required

`python: >= 3.5`

`neo4j: >= 4.3.0`

`tqdm: >= 4.10.0`

# Installation

`pip install neo4j-backup`

# Supported Neo4j Database Versions

`Neo4j >= 4.1`

# Usage

The exact parameters that should be used to access the database depends on the version of the Neo4j graph that you
are trying to access. The python neo4j-driver documentation can be found at 
https://neo4j.com/docs/api/python-driver/current/api.html.

There will be times when the script will ask the user for input for (y/N) questions, 
you can set `input_yes=True` to enter yes to all input questions.

# Constraints

The only constraint that is supported in all insistence of Neo4j are `Unique node property constraints`.
Currently, this is the only supported type of constraint in this codebase.
If you need to transfer the extracted data to an Enterprise edition database,
the other constraints can be added after importing the data
(assuming the data is structured in compliance with such constraints).

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
    indent_size = 4  # Indent of json files
    json_file_size: int = int("0xFFFF", 16)  # Size of data in memory before dumping
    extractor = Extractor(project_dir=project_dir, driver=driver, database=database,
                          input_yes=input_yes, compress=compress, indent_size=indent_size,
                          pull_uniqueness_constraints=True)
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

    database = "dev"

    project_dir = "data_dump"
    input_yes = False
    importer = Importer(project_dir=project_dir, driver=driver, database=database, input_yes=input_yes)
    importer.import_data()
```

# Data Storage

This example shows saved data from a Node with complex data types.

```json
{
    "node_id": 72,
    "node_labels": "Person:XX",
    "node_props": {
        "bool_example": false,
        "born": 1956,
        "int_example": 1,
        "point_3d_example": "$point({x: 3.0, y: 0.0, z: 2.0, crs: 'cartesian-3d'})",
        "localdatetime_example": "$datetime('2015-07-04T19:32:24.000000000+00:00')",
        "date_example": "$date('1999-01-01')",
        "point_2d_example": "$point({x: 3.0, y: 0.0, crs: 'cartesian'})",
        "datetime_example": "$datetime('2015-06-24T12:50:35.556000000+01:00')",
        "point_geo_3d_example": "$point({x: 56.0, y: 12.0, z: 2, crs: 'wgs-84-3d'})",
        "duration_example": "$duration('P5M1DT12H')",
        "odd_prop": "time('21:40:32.142000000+01:00')",
        "example_hash_prop": "e425c2703cf3d6d063f3de705c8f55a9be5e6fdee62bba4d95ce209352da2833",
        "name": "Tom Hanks",
        "localtime_example": "$time('12:50:35.556000000+00:00')",
        "point_geo_2d_example": "$point({x: 56.0, y: 12.0, crs: 'wgs-84'})",
        "float_example": 0.334,
        "time_example": "$time('21:40:32.142000000+01:00')",
        "array_example": [
            true,
            false
        ]
        },
    "hash_props": {
        "example_hash_prop": "$time('21:40:32.142000000+01:00')"
    }
}
```
Note that the `odd_prop` was originally stored as a string in the database.
Any string stored in the backup that starts with ["$point(", "$date(", "$time(", "$datetime(", "$duration("] 
represents a temporal or spatial value, and is stored as a literal value.
Any string that starts with a literal value is assumed to always be either a temporal or spatial value. 

If there happens to be a string that exists in the database that starts with a literal value string,
a sha256 has is generated from the property string and stored in `hash_props`.
For most neo4j databases, `hash_props` will be an empty dict for all nodes and relationships.
But, this step is taken to be sure the database backups cannot be intentionally or accidentally broken by 
oddly formatted strings.

Relationships are stored in a very similar fashion.
Example showing a Relationship with complex property values.

```json
{
    "rel_id": 224,
    "start_node_id": 71,
    "end_node_id": 150,
    "rel_type": "ACTED_IN",
    "rel_props": {
        "bool_example": false,
        "roles": [
            "Chuck Noland"
        ],
        "born": 1956,
        "int_example": 1,
        "point_3d_example": "$point({x: 3.0, y: 0.0, z: 2.0, crs: 'cartesian-3d'})",
        "localdatetime_example": "$datetime('2015-07-04T19:32:24.000000000+00:00')",
        "date_example": "$date('1999-01-01')",
        "point_2d_example": "$point({x: 3.0, y: 0.0, crs: 'cartesian'})",
        "datetime_example": "$datetime('2015-06-24T12:50:35.556000000+01:00')",
        "point_geo_3d_example": "$point({x: 56.0, y: 12.0, z: 2, crs: 'wgs-84-3d'})",
        "duration_example": "$duration('P5M1DT12H')",
        "odd_prop": "time('21:40:32.142000000+01:00')",
        "example_hash_prop": "e425c2703cf3d6d063f3de705c8f55a9be5e6fdee62bba4d95ce209352da2833",
        "name": "Tom Hanks",
        "localtime_example": "$time('12:50:35.556000000+00:00')",
        "point_geo_2d_example": "$point({x: 56.0, y: 12.0, crs: 'wgs-84'})",
        "float_example": 0.334,
        "time_example": "$time('21:40:32.142000000+01:00')",
        "array_example": [
            true,
            false
        ]
    },
    "hash_props": {
        "example_hash_prop": "$time('21:40:32.142000000+01:00')"
    }
}
```

The full list of supported property types to be extracted are:
Integer, Float, String, Boolean, Point, Date, Time, LocalTime, DateTime, LocalDateTime, and Duration.
As well as arrays, but arrays are treated as second class properties and have many restrictions in Neo4j.

Temporal values can be saved, but the python-neo4j driver makes no distinction between
- Time and LocalTime
- DateTime and LocalDateTime

The only difference with the time zone cannot be specified in local times.

The following point SRID types are supported and saved as:
- 7203 : 2d-cartesian-point
- 9157 : 3d-cartesian-point
- 4326 : 2d-WGS-84-point
- 4979 : 3d-WGS-84-point

All the data is extracted to the tree structure:
- data
  - nodes_<index>.json.gz -> list of nodes
  - nodes_<index>.json.gz
  - nodes_<index>.json.gz
  - ...
  - relationships_<index>.json.gz -> list of relationships
  - relationships_<index>.json.gz
  - relationships_<index>.json.gz
  - ...
- compressed.json -> A boolean indicating whether data is compressed or not
- db_id.json -> ID of db
- node_labels.json -> List of all Node labels
- property_keys.json -> List of all property keys
- rel_types.json -> List of all Relationship types
- uniqueness_constraints.json -> List of uniqueness constraints in Neo4j db
- unique_prop_key.json -> Some unique property that does not exist in db

# Notes About Importing Data into Neo4j

An internal ID property is made when creating Nodes and properties. 
Since this script does not read the underlying file in the Neo4j database, 
some unique identifier is needed to MATCH nodes on.
Forcing the user to pass a map of unique keys for each Node is not reasonable.
This temporary internal ID property is removed from each Node at the very end.
The Neo4j database still stores that this property existed at some point on a Node,
so the property will show up on the left side of the Neo4j Desktop and when running "CALL db.propertyKeys()".
The temporary property key can not be removed from the list of internal property keys, and the issue is reported at
https://github.com/neo4j/neo4j/issues/10941.

If you need to extract data from a database with this tool, 
and are importing to a database where you have access to the neo4j-admin console.
One option is to use the Importer from neo4j_import to import the initial data into a database,
then dump that database and restore it to a new database. 
Or, you can use a tool like store-utils https://github.com/jexp/store-utils.

This really is not so much an issue, more so as an inconvenience.
If you are using the Neo4j Desktop,
it is highly recommended to just use the built-in Dump tool.
