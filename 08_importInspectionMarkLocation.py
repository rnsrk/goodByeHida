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
    processedRows = pd.read_csv(f'./logs/processedInspectionMarkLocation.csv')
except FileNotFoundError:
    processedRows = pd.DataFrame(columns=['id', 'uuid', 'uri'])

# Load sources table
inspectionMarkLocationsTable = pd.read_sql_table('c__67b0_bz_dok_nr', con=engine)

inspectionMarkLocationValues = {}

# Create inspectionMarkLocations
for index, row in inspectionMarkLocationsTable.iterrows():
    # For every row in table...
    if index < len(processedRows) and inspectionMarkLocationsTable.iloc[index, 0] == processedRows.iloc[index, 0]:
        # skip if already processed
        print(f'Skipping already processed inspectionMarkLocation {inspectionMarkLocationsTable.iloc[index, 0]}')
        continue
    # Create Entity property dicts
    inspectionMarkLocationValues = {}
    for key, value in row.items():
        # For every column in row...
        if (value is None) or (value == ''):
            #  skip if cell has no value
            continue
        # Properties of an entity have to be an array, so...
        if '###{' in str(value):
            print('replaced curly braces')
            value = str(value).replace('###{new_line', '')
            value = str(value).replace('}###', '')
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
                inspectionMarkLocationValues['f65178b07306225efb0b556f6e4f54a5'] = value # UUID
            case 'f__67b0_bz_dok_nr':
                inspectionMarkLocationValues['f2d0b120ed40e17a5ad3f31d594d9b1c'] = value # Inspection Mark Identifier
            case 'f__67b4_anbr_ort':
                inspectionMarkLocationValues['f8a6343c2a8a5523eb2f0602f2baae04'] = value # Location

            case _:
                print(f'{key} is not a valid field, skipping.')

    # Create Material
    inspectionMarkLocation = Entity(api=api, fields=inspectionMarkLocationValues, bundle_id='b4158ec3a326d8ab504062296a82f13a')
    api.save(inspectionMarkLocation)

    print(f'Created inspectionMarkLocation {index}: {inspectionMarkLocation.uri}')

    # Write log
    processedRows = processedRows._append({'id': row['id'], 'uuid': inspectionMarkLocationValues['f65178b07306225efb0b556f6e4f54a5'][0], 'uri': inspectionMarkLocation.uri}, ignore_index=True)
    processedRows.to_csv(f'./logs/processedInspectionMarkLocation.csv', index=False)

print('finish')
