from pathlib import Path
from json import load, dumps
from time import sleep
from shutil import rmtree
from os import mkdir
from os.path import exists
import sys

import docker
from tqdm import tqdm
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, CypherSyntaxError

parent_dir = str(Path(__file__).resolve().parent.parent)
sys.path.append(parent_dir)

from src.neo4j_backup import Extractor, Importer
from src.neo4j_backup._backends import from_json


def test_core():

    # Base clinet
    client = docker.from_env()

    # Kill all currently running containers
    containers = client.containers.list()
    for container in containers:
        container.kill()

    # Delete and remake dumps 
    if exists(Path(__file__).parent / "dumps"):
        rmtree(Path(__file__).parent / "dumps")
    mkdir(Path(__file__).parent / "dumps")

    # Load tags to pull
    with open(Path(__file__).parent / "tags.json", "r") as f:
        tags = set(load(f))

    # Check what images have already been pulled from list
    images = client.images.list()

    # Loop through the images, and save tags
    already_pulled_tags = set()
    for image in images:
        for tag in image.tags:
            if tag.split(":")[1] in tags and tag.split(":")[0] == "neo4j":
                already_pulled_tags.add(tag.split(":")[1])

    # Pull tags to need to be pulled
    tags_to_pull = tags - already_pulled_tags
    for tag in tqdm(tags_to_pull, desc=f"Pulling Neo4j docker images"):
        client.images.pull(f'neo4j:{tag}')

    # Loop through the all tags, and run test
    for tag in tags:

        print("------------------------------------------------------------")
        print(f"Running core data dump/import test on neo4j:{tag}")
        print("------------------------------------------------------------")

        # Run container
        container = client.containers.run(f'neo4j:{tag}', 
                                        detach=True, 
                                        ports={'7474/tcp': 7474, '7687/tcp': 7687},
                                        environment={'NEO4J_AUTH': 'neo4j/password'}
                                        )
        
        # Database driver
        uri = "neo4j://localhost:7687"
        username = "neo4j"
        password = "password"
        driver = GraphDatabase.driver(uri, auth=(username, password), encrypted=False)

        # Load queries
        with open(Path(__file__).parent / "queries/movies.cypher", "r") as f:
            movies_query = f.read()
        with open(Path(__file__).parent / "queries/modify_node.cypher", "r") as f:
            modifiy_node = f.read()
        with open(Path(__file__).parent / "queries/modify_rel.cypher", "r") as f:
            modifiy_rel = f.read()
        with open(Path(__file__).parent / "queries/lonely_node.cypher", "r") as f:
            lonely_node = f.read()
        with open(Path(__file__).parent / "queries/v5_id_as_prop.cypher", "r") as f:
            v5_id_as_prop = f.read()
        with open(Path(__file__).parent / "queries/v4_id_as_prop.cypher", "r") as f:
            v4_id_as_prop = f.read()
        with open(Path(__file__).parent / "queries/v5_constraint.cypher", "r") as f:
            v5_contraint = f.read()
        with open(Path(__file__).parent / "queries/v4_constraint.cypher", "r") as f:
            v4_contraint = f.read()

        # Create a graph with known input data, extract data
        while True:
            print(f"Attempting to connect to orginial neo4j:{tag}")
            try:

                with driver.session(database="neo4j") as session:

                    # Load in inital data
                    session.run(movies_query)
                    session.run(modifiy_node)
                    session.run(modifiy_rel)
                    session.run(lonely_node)

                    try:
                        session.run(v5_id_as_prop)
                        session.run(v5_contraint)
                    except CypherSyntaxError:
                        session.run(v4_id_as_prop)
                        session.run(v4_contraint)
                    
                    # Extract out data in Neo4j
                    extractor = Extractor(project_dir=Path(__file__).parent / f"dumps/main_{tag}", driver=driver, database="neo4j",
                                          input_yes=True, compress=True, indent_size=0, json_file_size=int("0xFF", 16), pull_uniqueness_constraints=True)
                    extractor.extract_data()

                break
            except ServiceUnavailable:
                sleep(5)

        # Destory previous graph
        container.kill()
        container = client.containers.run(f'neo4j:{tag}', 
                                detach=True, 
                                ports={'7474/tcp': 7474, '7687/tcp': 7687},
                                environment={'NEO4J_AUTH': 'neo4j/password'}
                                )
        driver = GraphDatabase.driver(uri, auth=(username, password), encrypted=False)

        # Input extracted data into new graph, extract data again
        while True:
            print(f"Attempting to connect to duplicate neo4j:{tag}")
            try:
                with driver.session(database="neo4j") as session:

                    # Import extracted data back into the empty database
                    importer = Importer(project_dir=Path(__file__).parent / f"dumps/main_{tag}", driver=driver, database="neo4j", 
                                        input_yes=True)
                    importer.import_data()

                    # Extract out imported data
                    extractor = Extractor(project_dir=Path(__file__).parent / f"dumps/duplicate_{tag}", driver=driver, database="neo4j",
                                          input_yes=True, compress=True, indent_size=0, json_file_size=int("0xFF", 16), pull_uniqueness_constraints=True)
                    extractor.extract_data()

                break
            except ServiceUnavailable:
                sleep(5)

        # Kill container again
        container.kill()


def _reindex(nodes, rels):
    node_dict = dict()
    for node in nodes:
        current_node_id = node["node_id"]
        true_node_id = node["node_props"]["node_id"]
        node_dict[current_node_id] = true_node_id

    new_nodes = []
    for node in nodes:
        node["node_id"] = node_dict[node["node_id"]]
        node["node_props"].pop("node_id")
        new_nodes.append(node)

    new_rels = list()
    for rel in rels:
        rel["rel_id"] = rel["rel_props"]["rel_id"]
        rel["rel_props"].pop("rel_id")
        rel["start_node_id"] = node_dict[rel["start_node_id"]]
        rel["end_node_id"] = node_dict[rel["end_node_id"]]
        new_rels.append(rel)

    return new_nodes, new_rels

def _pull_dicts(dump):
    nodes = []
    rels = []
    for file in dump.iterdir():
        if "node" in file.name:
            nodes.extend(from_json(file, compressed=True))
        elif "rel" in file.name:
            rels.extend(from_json(file, compressed=True))
    nodes, rels = _reindex(nodes, rels)
    return nodes, rels


def _test_equiv(list1, list2):
    # Verify that list1 is equal to list2, even if the items are unsorted
    assert len(list1) == len(list2)

    sort_key = lambda x: tuple(x[k] for k in list(list1[0].keys()))

    sorted_list1 = sorted(list1, key=sort_key)
    sorted_list2 = sorted(list2, key=sort_key)
    for dict1, dict2 in zip(sorted_list1, sorted_list2):
        dict1 = dumps(dict1, sort_keys=True)
        dict2 = dumps(dict2, sort_keys=True)
        assert dict1 == dict2


def test_equvilant_nodes():

    # Load tags
    with open(Path(__file__).parent / "tags.json", "r") as f:
        tags = set(load(f))

    for tag in tags:
        main_dir = Path(__file__).parent / f"dumps/main_{tag}/data"
        main_nodes, main_rels = _pull_dicts(main_dir)
        
        dup_dir = Path(__file__).parent / f"dumps/duplicate_{tag}/data"
        dup_nodes, dup_rels = _pull_dicts(dup_dir)

        _test_equiv(main_nodes, dup_nodes)


def test_equvilant_rels():

    # Load tags
    with open(Path(__file__).parent / "tags.json", "r") as f:
        tags = set(load(f))

    for tag in tags:
        main_dir = Path(__file__).parent / f"dumps/main_{tag}/data"
        main_nodes, main_rels = _pull_dicts(main_dir)
        
        dup_dir = Path(__file__).parent / f"dumps/duplicate_{tag}/data"
        dup_nodes, dup_rels = _pull_dicts(dup_dir)

        _test_equiv(main_rels, dup_rels)


def test_equvilant_meta():

    with open(Path(__file__).parent / "tags.json", "r") as f:
        tags = set(load(f)) 

    ignore = ["unique_prop_key.json", "db_id.json", "compressed.json"]

    for tag in tags:
        main_dir = Path(__file__).parent / f"dumps/main_{tag}"
        dup_dir = Path(__file__).parent / f"dumps/duplicate_{tag}"

        for file in main_dir.iterdir():
            if file.is_file() and file.name not in ignore:
                with open(file, "r") as f:
                    data1 = load(f)
                with open(dup_dir / file.name) as f:
                    data2 = load(f)

                if not data1:
                    assert data1 == data2
                elif isinstance(data1, list):
                    if isinstance(data1[0], dict):
                        sorted_list1 = sorted(data1, key=lambda d: str(sorted(d.items())))
                        sorted_list2 = sorted(data2, key=lambda d: str(sorted(d.items())))
                        for dict1, dict2 in zip(sorted_list1, sorted_list2):
                            assert dict1 == dict2
                    else:
                        sorted_list1 = sorted(data1)
                        sorted_list2 = sorted(data2)
                        assert sorted_list1 == sorted_list2
                else:
                    assert data1 == data2 


if __name__ == "__main__":

    test_core()
    test_equvilant_nodes()
    test_equvilant_rels()
    test_equvilant_meta()
