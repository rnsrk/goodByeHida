# Good Bye HIDA
Small script to transform XML Documents of the HIDA/MIDAS architecture to a sqlite database.

## Prerequisites
create a virtual environment:
```bash
python3 -m venv venv
```
activate the virtual environment:
```bash
source venv/bin/activate
```
install requirements:
```bash
pip install -r requirements.txt
```
place the XML files in the `docs` folder or for evaluation purposes few files in the `test-docs` folder.

## Purpose
The program iterates through the docs dir and in a first run, it builds database schemas from the 
structure of the XML files, which will be used to create a data model for an SQLlite databse.
In a second iteration, every node with children or node with a "txt" element will be an entity (beginning with c__)
all other elements will be attributes (beginning with f__). Entities will be connected with an uuid
foreign key to their parent entity in relational tables (beginning with r__).
## Usage
To have a test run, place XML-files in a dir named `test-docs`, then type
```bash
python3 goodByeHida.py --buildSchemas True 
```
You will get a dir `test-schemas` and a sqlite database `test.db` with the imported data.

If everything looks good you can run the script with the `docs` folder:
```bash
python3 goodByeHida.py --production True --buildSchemas True 
```
You will get a dir `schemas` and a sqlite database `databse.db` with the imported data.

If you like to restart the process and delete the database, type:
```bash
python3 goodByeHida.py --production True --buildSchemas True --deleteDatabase True
```
