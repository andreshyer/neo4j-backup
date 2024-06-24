# Unittests

These scripts will become full unittest that are intergrated into Github.
For now, test.py acts as a means of making sure that current code is working.

## Installation (Linux)

Docker must be installed, https://docs.docker.com/engine/install/ubuntu/.
Afterwards, the user must be added to usergroup.
Otherwise, you must run `test.py` as sudo.

```
sudo groupadd docker
sudo usermod -aG docker $USER
newgrp docker
groups
```

Where $USER should be replaced with your actual username.
If docker has been installed correctly, your username should be listed.

## Usage

Afterward, all that should be left is to run `test.py`.
If `test.py` runs correctly, than the code is currently working.
Otherwise, this script can help act as a guide as to what is breaking.

## Tested Tags

These are the list of different tags that have been directly tested

["4.1.0-community", "4.4.0-community", "5-community", "5.5.0-community", "5.10.0-community", "5.15.0-community", "5.20.0-community", "latest"]
