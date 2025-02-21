import uuid # For UUID creation
from initDb import initDb # For database initialization
from wisski.api import Api, Pathbuilder, Entity # For WissKI API
import os # For environment variable loading
from dotenv import load_dotenv # For environment variable loading
import pandas as pd # For dataframe handling

# Initialize the database
print('Initializing the database...')
engine, metadata, Session = initDb(True, './schemas/')
if engine == False:
    print('Database initialization failed.')
    exit()

# Load the environment variables
load_dotenv()

# Initialize the WissKI API
print('Initializing the WissKI API...')
api_url = os.getenv('API_URL')
auth = (os.getenv('API_USERNAME'), os.getenv('API_PASSWORD'))
headers = {"Cache-Control": "no-cache"}
api = Api(api_url, auth, headers)
api.pathbuilder = api.get_pathbuilder('default')

try:
    processedRows = pd.read_csv(f'./logs/processedInspectionMarkRelation.csv')
except FileNotFoundError:
    processedRows = pd.DataFrame(columns=[ 'uuid', 'uri'])

# Load sources table
inspectionMarkRelationsTable = pd.read_sql_table('c__67b7_beziehung', con=engine)

inspectionMarkRelationValues = {}

# Create inspectionMarkRelations
for index, row in inspectionMarkRelationsTable.iterrows():
    # For every row in table...
    if index < len(processedRows) and inspectionMarkRelationsTable.iloc[index, 0] == processedRows.iloc[index, 0]:
        # skip if already processed
        print(f'Skipping already processed inspectionMarkRelation {inspectionMarkRelationsTable.iloc[index, 0]}')
        continue
    # Create Entity property dicts
    inspectionMarkRelationValues = {}
    for key, value in row.items():
        # For every column in row...
        if (value is None) or (value == ''):
            #  skip if cell has no value
            continue
        # Properties of an entity have to be an array, so...
        if '###{{new_line}}###' in str(value):
            print('replaced curly braces')
            value = str(value).replace('###{{new_line}}###', '')
        if '&' in str(value):
            # ...Explode "&"-separated values to array items
            value = [x.strip() for x in str(value).split('&')]
        else:
            # ...Or parse to array
            value = [value]
        # Map columns to fields. We use assignments for reification.
        match key:
            case 'id':
                continue
            case 'f__uuid':
                inspectionMarkRelationValues['ffd502413c286815811ae5546f73935b'] = value # UUID
            case 'f__67b8_bez_bz_nr':
                inspectionMarkRelationValues['ff3f6dd331ed27515f6721ac8312706c'] = value # Inspection Mark Identifier
            case 'f__67b7_beziehung':
                inspectionMarkRelationValues['f1cb8db7e1c26a5b5fe0c9d8fca60de2'] = value # Relation

            case _:
                print(f'{key} is not a valid field, skipping.')

    # Create Material
    inspectionMarkRelation = Entity(api=api, fields=inspectionMarkRelationValues, bundle_id='bd9b0ff8dc3a6d9284e1798531389bf1')
    api.save(inspectionMarkRelation)

    print(f'Created inspectionMarkRelation {index}: {inspectionMarkRelation.uri}')

    # Write log
    processedRows = processedRows._append({'uuid': inspectionMarkRelationValues['ffd502413c286815811ae5546f73935b'][0], 'uri': inspectionMarkRelation.uri}, ignore_index=True)
    processedRows.to_csv(f'./logs/processedInspectionMarkRelation.csv', index=False)

print('finish')
