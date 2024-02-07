import argparse
from buildSchemas import buildSchemas
from distutils.util import strtobool
from importer import Importer
from initDb import initDb
import os

# Create the parser
parser = argparse.ArgumentParser(description="Run the program with specific configurations.")

# Add the arguments
parser.add_argument('--production', type=str, default='False', help='Set to True if you want to parse the docs folder, else if parse test-docs')
parser.add_argument('--buildSchemas', type=str, default='False', help='Set to True to rebuild the JSONs for the database schemas')
parser.add_argument('--dropDb', type=str, default='False', help='Set to True to drop the database to restart from scratch')

# Parse the arguments
args = parser.parse_args()

_production = bool(strtobool(args.production))
_buildSchemas = bool(strtobool(args.buildSchemas))
_dropDb = bool(strtobool(args.dropDb))

if _production:
    print('Running in production mode.')
    docsDir: str = './docs/'  # The directory containing the XML files.
    schemaDir: str = './schemas/'  # The directory to store the schemas.
else:
    print('Running in test mode.')
    docsDir = './test-docs/'
    schemaDir = './test-schemas/'

if _buildSchemas:
    print('Creating the schema jsons...')
    buildSchemas(docsDir, schemaDir)

if _dropDb:
    # Renew the database
    print('Remove the database...')
    if _production:
        dbName = 'database.db'
    else:
        dbName = 'test.db'
    if os.path.exists(dbName):
        os.remove(dbName)
        print('Database removed.')
    else:
        print('Database does not exist.')


# Initialize the database
print('Initializing the database...')
engine, metadata = initDb(_production, schemaDir)
if engine == False:
    print('Database initialization failed.')
    exit()


# Import the data
print('Importing the data...')
importer = Importer(engine, metadata, docsDir)
importer.importData()

print('Finished.')

