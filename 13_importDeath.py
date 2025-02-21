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


tableName = "c__3330_todes_dat_"
bundleId = 'b487c08016f572b9ecf3f9173339fec3'

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
    if index < len(processedRows) and sqlTable.iloc[index, 'docId'] == processedRows.iloc[index, 'docId']:
        # skip if already processed
        print(f'Skipping already processed entity {sqlTable.iloc[index, 0]}')
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
                entityValues['f8beb0d372a5cf6f1668c47acf7e53cd'] = value # UUID
                uuid = value[0]
            case 'f__3330_todes_dat_':
                entityValues['f385a8c323f0a2f49d8eb175e1535b1b'] = value # Death date
            case 'f__33ls_lit__stelle':
                entityValues['fb4f168aa6a73169ef0350408a6260cc'] = value # Literature Reference
            case 'f__33lt_lit__kurztitel':
                entityValues['fd4ed8828d72a575f8609ba2c442b4b2'] = value # Literature short title
            case 'f__33bm_bem_tod':
                entityValues['f3028661430081ae44aa950abe0afbac'] = value # Note
            case 'f__3350_tod_ort':
                entityValues['fd80c2c8ba4c64c01e9c46ac7ae00d93'] = value # Place
            case 'f__33qs_quelle_stelle':
                entityValues['fd98cf7fbc0de4529e2a2d5e0b0c28bf'] = value # Source reference
            case 'f__33qt_quelle_kurztitel':
                entityValues['f973818e6c3d36ddd44ba3a713e308e6'] = value # Source short title
            case 'f__710t_art_ereignis':
                entityValues['fc039c43502b3525a92a8330d91f7944'] = value # Event type
            case _:
                print(f'{key} is not a valid field, skipping.')

    # Create Material
    entity = Entity(api=api, fields=entityValues, bundle_id=bundleId)
    api.save(entity)

    print(f'Created entity {index}: {entity.uri} of {len(sqlTable)}')

    # Write log
    processedRows = processedRows._append({'docId': docId, 'uuid': fUuid, 'uri': entity.uri}, ignore_index=True)
    processedRows.to_csv(f'./logs/processed-{tableName}.csv', index=False)

print('finish')
