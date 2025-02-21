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
api.pathbuilders = ['default']

try:
    processedRows = pd.read_csv(f'./logs/processedMaterials.csv')
except FileNotFoundError:
    processedRows = pd.DataFrame(columns=['id', 'uuid', 'uri'])

# Load materials table
materialsTable = pd.read_sql_table('c__5280_material', con=engine)

# Create materials
for index, row in materialsTable.iterrows():
    # For every row in table...
    if index < len(processedRows) and materialsTable.loc[index, 'id'] == processedRows.iloc[index, 'id']:
        # skip if already processed
        print(f'Skipping already processed material {materialsTable.iloc[index, 0]}')
        continue
    # Create Entity property dicts
    materialValues = {}
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
                materialValues['fedfe553c2332bd4902c887813f29ed8'] = value # UUID
            case 'f__5280_material':
                materialValues['f5f4251312f54c0d104ea87761b94bde'] = value # Material
            case 'f__5300_technik':
                materialValues['f231e08850022f091ebd5055d8aad30f'] = value # Technique
            case _:
                print(f'{key} is not a valid field, skipping.')

    # Create Material
    material = Entity(api=api, fields=materialValues, bundle_id='b45978f2b073ff3c73b3c7220ebb3b89')
    api.save(material)

    print(f'Created material {index}: {material.uri}')

    # Write log
    processedRows = processedRows._append({'id': row['id'], 'uuid': materialValues['fedfe553c2332bd4902c887813f29ed8'][0], 'uri': material.uri}, ignore_index=True)
    processedRows.to_csv(f'./logs/processedMaterials.csv', index=False)

print('finish')
