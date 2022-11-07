from random import choice
from string import ascii_lowercase
from json import dumps, loads
from gzip import open as gzip_open


def to_json(file_path, data, compress=False):
    json_string = dumps(data, default=str, indent=4)

    if compress:
        with gzip_open(f"{file_path}.gz", 'wb') as f:
            f.write(bytes(json_string, 'utf-8'))

    else:
        with open(f"{file_path}", "w") as f:
            f.write(json_string)


def from_json(file_path, compressed=False):
    if compressed:
        with gzip_open(file_path, 'rb') as f:
            json_string = f.read()
        data = loads(json_string)

    else:
        with open(file_path, "r") as f:
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
