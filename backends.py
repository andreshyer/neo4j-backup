from os import listdir
from random import choice
from string import ascii_lowercase
from json import dumps, loads
from gzip import open as gzip_open


def compress_json(file_path, data):
    json_string = dumps(data)
    with gzip_open(f"{file_path}.gz", "w") as f:
        f.write(bytes(json_string, 'utf-8'))


def decompress_json(file_path):
    with gzip_open(file_path, "r") as f:
        json_string = f.read()
    data = loads(json_string)
    return data


def get_unique_prop_key(properties):
    # Generate random string using lowercase ascii letter that is 16 letters long
    def random_string_generator(str_size, allowed_chars):
        return ''.join(choice(allowed_chars) for _ in range(str_size))

    # Cast properties to lowercase
    properties = [prop.lower() for prop in properties]

    # Keep running until a unique property is found
    while True:
        unique_prop_key = random_string_generator(16, ascii_lowercase)
        if unique_prop_key not in properties:
            break
    return unique_prop_key


def number_to_letters(n: int):
    """
    Simple code that converts a number > 0 into letters similar to how excel labels columns.
    0 = a, 1 = b, ... 25 = z, 26 = aa, 27 = ab, 28 = ac, ....
    The source for this code can be found at
    https://stackoverflow.com/questions/48983939/convert-a-number-to-excel-s-base-26
    :param n: Integer equal to or greater than 0
    :return: Letters corresponding to the number passed
    """
    if n < 0:
        raise TypeError(f"n must be equal to or greater than 0, a value of {n} was passed")
    n += 1

    def div_mod_excel(i):
        a, b = divmod(i, 26)
        if b == 0:
            return a - 1, b + 26
        return a, b

    chars = []
    while n > 0:
        n, d = div_mod_excel(n)
        chars.append(ascii_lowercase[d - 1])
    return ''.join(reversed(chars))


def gather_labels(project_dir_path, relationships):

    node_files = []
    for file_path in listdir(project_dir_path):
        if "node" in file_path:
            file_path = project_dir_path / file_path
            node_files.append(file_path)

    new_relationships = []
    for node_file in node_files:
        nodes = decompress_json(node_file)
        node_keys = list(nodes.keys())
        for relationship in relationships:

            if relationship['start_node'] in node_keys:
                relationship['start_node_labels'] = nodes[relationship['start_node']]['labels']

            if relationship['end_node'] in node_keys:
                relationship['end_node_labels'] = nodes[relationship['end_node']]['labels']

            new_relationships.append(relationship)

    return new_relationships


def format_props(props):

    formatted_props = []
    for prop_key, prop_value in props.items():
        formatted_props.append({'key': prop_key, 'value': prop_value})
    return formatted_props
