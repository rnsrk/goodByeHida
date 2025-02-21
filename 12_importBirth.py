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

test = False
tableName = "c__3270_geb_datum"
bundleId = 'b54049ec931bffb62359b4bdb11435fc'

try:
    processedRows = pd.read_csv(f'./logs/processed-{tableName}.csv')
except FileNotFoundError:
    processedRows = pd.DataFrame(columns=['docId', 'uuid', 'uri'])

# Load sources table
sqlTable = pd.read_sql_table(tableName, con=engine)

entityValues = {}

# Create entities
for index, row in sqlTable.iterrows():
    # For every row in table...
    if index < len(processedRows) and sqlTable.loc[index, 'id'] == processedRows.loc[index, 'docId']:
        # skip if already processed
        print(f'Skipping already processed entity {sqlTable.loc[index, 'id']}')
        continue
    # Create Entity property dicts
    entityValues = {}
    for key, value in row.items():
        # For every column in row...
        if (value is None) or (value == ''):
            #  skip if cell has no value
            continue
        # Properties of an entity have to be an array, so...
        if '&' in str(value):
            # ...Explode "&"-separated values to array items
            value = [x.strip() for x in str(value).split('&')]
        else:
            # ...Or parse to array
            value = [value]
        # Map columns to fields. We use assignments for reification.
        docId = ''
        match key:
            case 'id':
                docId = value[0]
            case 'f__uuid':
                entityValues['ff2a4da76944f5aba7d625c169d9ff66'] = value # UUID
                fUuid = value[0]
            case 'f__3290_geb_ort':
                entityValues['fe71d86a78289c0b54242f5a3b67f81f'] = value # Birth place
            case 'f__3270_geb_datum':
                entityValues['ff3a9f042976963ac356db02d764b002'] = value # Date
            case 'f__32ls_lit__stelle':
                entityValues['fa03638df8a53e9aae38471fe10f409a'] = value # Literature Reference
            case 'f__32lt_lit__kurztitel':
                entityValues['f1af25f1770bd0db1982780697600cf4'] = value # Literature short title
            case 'f__32bm_bem_geburt':
                entityValues['f572f5e0f02f1c9b7c3ece5ffcf86c43'] = value # Note
            case 'f__32qs_quelle_stelle':
                entityValues['f1ebceaa76bac9ebf266733f64caa37c'] = value # Source reference
            case 'f__32qt_quelle_kurztitel':
                entityValues['f1a3597a874b3df9c1d87c5a32b487b0'] = value # Source short title
            case _:
                print(f'{key} is not a valid field, skipping.')

    # Create Material
    entity = Entity(api=api, fields=entityValues, bundle_id=bundleId)
    api.save(entity)

    print(f'Created entity {index}: {entity.uri} of {len(sqlTable)}')

    # Write log
    processedRows = processedRows._append({'docId': docId, 'uuid': fUuid, 'uri': entity.uri}, ignore_index=True)
    processedRows.to_csv(f'./logs/processed-{tableName}.csv', index=False)
    if test:
        exit()

print('finish')
