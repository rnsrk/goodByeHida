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

tableName = "r__lit__8292_uebergeordn_publ_"
bundleId = 'b2adaaa15714d83ea83cd3333af437df' # Literature to parent publication relation

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
            case 'f__lit__uuid':
                entityValues['f1ecd1cf9be1081507f9c8f3758bafe9'] = value # Date
                fUuid = value[0]
            case 'f__8292_uebergeordn_publ___uuid':
                entityValues['f9997e4bbacb1c26a945825cfe5b6de2'] = value #
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
