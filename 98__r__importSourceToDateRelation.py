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
api.pathbuilder = api.get_pathbuilder('relations')

test = False

tableName = "r__que__8100_datum"
bundleId = 'b4b8ba242075bf2c778894911c7f3264' # Source to date relation

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
            case 'f__que__uuid':
                entityValues['f2e8d1b76c8b196c8deb9e0abe90a5b3'] = value # Source UUID
                fUuid = value[0]
            case 'f__8100_datum__uuid':
                entityValues['ff5ac62e6327599566d4474e18423265'] = value # Date UUID
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
