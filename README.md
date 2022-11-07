# neo4j-backup
This is a project that is designed at downloading and uploading data in Neo4j Knowledge Graphs without Neo4j 
Enterprise edition. This is useful if Dump files cannot be used, or if you want to download the data that exist in
a Neo4j database in a human-readable format.

# Overview

This repo is not intended to replace the native Neo4j backup Dump files,
but rather to be used in instances where a Dump file is not an option.
Such as moving data from Neo4j to a different type of database.

Also, this repo aims to be as simplistic as possible with two main purposes. 
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

There will be times when the script will ask the user for input for (y/N) questions, 
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

    project_dir = "data_Dump"
    input_yes = False
    compress = True
    extractor = Extractor(project_dir="data_Dump", driver=driver, database=database,
                          input_yes=input_yes, compress=compress)
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

    project_dir = "data_Dump"
    input_yes = False
    importer = Importer(project_dir="data_Dump", driver=driver, database=database, input_yes=input_yes)
    importer.import_data()
```

# Data Storage

All property types can be stored in JSON format, the json encoder is set to "str".
The default `str` encoder is used for extracting data into json files. 
This allows for saving complex data types in Neo4j, such as points and temporal values
(Date, DateTime, Time).

```python
from datetime import datetime
from json import dumps
data = {"extracted_data": datetime.now()}  # This would be real extracted data
json_string = dumps(data, default=str)
```

This example shows saved data from a Node with complex data types.

```json
{
        "node_id": 71,
        "node_labels": [
            "Person"
        ],
        "node_props": {
            "bool_example": false,
            "born": 1956,
            "int_example": 1,
            "datatime_example": "2015-06-24T12:50:35.556000000+01:00",
            "point_3d_example": [
                3.0,
                0.0,
                3.4
            ],
            "localdatetime_example": "2015-07-04T19:32:24.000000000",
            "duration": {
                "months": 0,
                "days": 0,
                "seconds": 0,
                "nanoseconds": 1
            },
            "date_example": "1999-01-01",
            "point_2d_example": [
                3.0,
                0.0
            ],
            "point_geo_3d_example": [
                56.0,
                11.0,
                1000.0
            ],
            "name": "Tom Hanks",
            "localtime_example": "12:50:35.556000000",
            "point_geo_2d_example": [
                56.0,
                12.0
            ],
            "array_example": [
                true
            ],
            "float_example": 0.334,
            "time_example": "21:40:32.142000000+01:00"
        },
        "node_props_types": {
            "bool_example": "bool",
            "born": "int",
            "int_example": "int",
            "datatime_example": "datetime",
            "point_3d_example": "3d-cartesian-point",
            "localdatetime_example": "datetime",
            "duration": "duration",
            "date_example": "date",
            "point_2d_example": "2d-cartesian-point",
            "point_geo_3d_example": "3d-WGS-84-point",
            "name": "str",
            "localtime_example": "time",
            "point_geo_2d_example": "2d-WGS-84-point",
            "array_example": [
                "bool"
            ],
            "float_example": "float",
            "time_example": "time"
        }
    }
```

The properties saved for relationships are very similar.
An example relationship is stored as:

```json
{
        "start_node_id": 71,
        "start_node_labels": [
            "Person"
        ],
        "end_node_id": 85,
        "end_node_labels": [
            "Movie"
        ],
        "rel_type": "DIRECTED",
        "rel_props": {
            "bool_example": false,
            "int_example": 1,
            "datatime_example": "2015-06-24T12:50:35.556000000+01:00",
            "point_3d_example": [
                3.0,
                0.0,
                3.4
            ],
            "localdatetime_example": "2015-07-04T19:32:24.000000000",
            "duration": {
                "months": 0,
                "days": 0,
                "seconds": 0,
                "nanoseconds": 1
            },
            "date_example": "1999-01-01",
            "point_2d_example": [
                3.0,
                0.0
            ],
            "point_geo_3d_example": [
                56.0,
                11.0,
                1000.0
            ],
            "localtime_example": "12:50:35.556000000+00:00",
            "point_geo_2d_example": [
                56.0,
                12.0
            ],
            "float_example": 0.334,
            "time_example": "21:40:32.142000000+01:00"
        },
        "rel_props_types": {
            "bool_example": "bool",
            "int_example": "int",
            "datatime_example": "datetime",
            "point_3d_example": "3d-cartesian-point",
            "localdatetime_example": "datetime",
            "duration": "duration",
            "date_example": "date",
            "point_2d_example": "2d-cartesian-point",
            "point_geo_3d_example": "3d-WGS-84-point",
            "localtime_example": "time",
            "point_geo_2d_example": "2d-WGS-84-point",
            "float_example": "float",
            "time_example": "time"
        }
    }
```

The full list of supported property types to be extracted are:
Integer, Float, String, Boolean, Point, Date, Time, LocalTime, DateTime, LocalDateTime, and Duration.
As well as arrays, but arrays are treated as second class properties and have many restrictions in Neo4j.

The type are saved as:
int, float, str, bool, date, time, datetime, duration, 
2d-cartesian-point, 3d-cartesian-point, 2d-WGS-84-point, and 3d-WGS-84-point.

While Temporal values can be saved, the python-neo4j driver makes no distinction between
- Time and LocalTime
- DateTime and LocalDateTime

The only difference with the prefix local being that when creating the property,
Neo4j will first convert local times to global times.

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
- compressed.json -> bool weather or not data is compresses
- constraints.json -> List of constraints
- constraints_names.json -> Names of constraints in Neo4j db
- db_id.json -> ID of db
- node_labels.json -> List of all Node labels
- property_keys.json -> List of all property keys
- rel_types.json -> List of all Relationship types
- unique_prop_key.json -> Some unique property that does not exist in db

# Notes About Importing Data into Neo4j

This may not be the best tool to back up data if speed is a concern.
This tool is significantly slower than the built-in Dump tool Neo4j provides.
The selling point of this script is also its biggest downfall, all calls to Neo4j are done with Cypher.
This adds a significant amount of overhead that can be avoided if the direct files of a graph can be accessed.
Also, while the raw data is machine-readable, 
it still needs to be manipulated by the end user to insert it into other databases.

Another note, an internal ID property is made when creating Nodes and properties. 
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
