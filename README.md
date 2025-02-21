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
structure of the XML files, which will be used to create a data model for an SQLite database.
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
You will get a dir `schemas` and a sqlite database `database.db` with the imported data.

If you like to restart the process and delete the database, type:
```bash
python3 goodByeHida.py --production True --buildSchemas True --deleteDatabase True
```


## Import data WissKI
Run skripts in this order:
 importMaterials.py
 importAdministrator.py
 importSource.py
 importLiterature.py
 importArtist.py
 importWorkshops.py
 importAdministratorStatus.py
 importArtistRelation.py
 importMarks.py
 importInspectionMarks.py
 importInspectionMarkLocation.py
 importInspectionMarkRelation.py
 importMarkDatingInfo.py
 importSourceReference.py
 importClient.py
 importGoldsmithRelation.py
 importOrigin.py
 importBirth.py
 importDeath.py
 importArtifactRelation.py
 importNumDation.py
 importArtifacts.py
 ImportMarkInfo.py
 importMarkInformation.py
 importArtifactToMarkAssignments.py

## Roadmap
### From XML to database
- [x] Build database schemas from XML files
- [x] Parse HIDA/MIDAS XML files to SQL database
### From database to WissKI
- [x] Importer for material
- [x] Importer for artifacts
- [x] Importer for marks
- [x] Importer for mark information
- [x] Importer for artifact to mark assignments
- [ ] Importer for artists
- [ ] Importer for inspection marks
- [ ] Importer for literature
- [ ] Importer for continuation of the workshop
- [ ] Importer for relation to artist

### Other
- [ ] Reduce redundancy by importing features: first collect features possibilies in own table and then import them in a second step