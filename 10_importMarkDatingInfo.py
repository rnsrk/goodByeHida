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
    processedRows = pd.read_csv(f'./logs/processedDatingInfo.csv')
except FileNotFoundError:
    processedRows = pd.DataFrame(columns=[ 'uuid', 'uri'])

# Load sources table
datingInfosTable = pd.read_sql_table('c__68dm_datierung_marke', con=engine)

datingInfoValues = {}

# Create datingInfos
for index, row in datingInfosTable.iterrows():
    # For every row in table...
    if index < len(processedRows) and datingInfosTable.iloc[index, 0] == processedRows.iloc[index, 0]:
        # skip if already processed
        print(f'Skipping already processed datingInfo {datingInfosTable.iloc[index, 0]}')
        continue
    # Create Entity property dicts
    datingInfoValues = {}
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
                datingInfoValues['f74baaf58e49393cc89d6616ee197901'] = value # UUID
            case 'f__68dm_datierung_marke':
                datingInfoValues['f0da3b36d16e16602bb550aff7d36297'] = value # Date
            case 'f__68bm_bem_dat_marke':
                datingInfoValues['fe7870b5a86040d81140bccb01697765'] = value # Note

            case _:
                print(f'{key} is not a valid field, skipping.')

    # Create Material
    datingInfo = Entity(api=api, fields=datingInfoValues, bundle_id='b9cfb95e627e1710cf8d736d4ca5db64') #Dating Information Assignment
    api.save(datingInfo)

    print(f'Created datingInfo {index}: {datingInfo.uri} of {len(datingInfosTable)}')

    # Write log
    processedRows = processedRows._append({'uuid': datingInfoValues['f74baaf58e49393cc89d6616ee197901'][0], 'uri': datingInfo.uri}, ignore_index=True)
    processedRows.to_csv(f'./logs/processedDatingInfo.csv', index=False)

print('finish')
