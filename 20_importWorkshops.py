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
    processedRows = pd.read_csv(f'./logs/processedWorkshops.csv')
except FileNotFoundError:
    processedRows = pd.DataFrame(columns=[ 'uuid', 'uri'])

test = False
# Load sources table
workshopsTable = pd.read_sql_table('c__nfws_forts_werkst_', con=engine)

workshopValues = {}

# Create workshops
for index, row in workshopsTable.iterrows():
    # For every row in table...
    if index < len(processedRows) and workshopsTable.iloc[index, 0] == processedRows.iloc[index, 0]:
        # skip if already processed
        print(f'Skipping already processed workshop {workshopsTable.iloc[index, 0]}')
        continue
    # Create Entity property dicts
    workshopValues = {}
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
        match key:
            case 'id':
                docId = value[0]
            case 'f__uuid':
                workshopValues['fa7c19f4d03d7d15acf588460654bbf2'] = value # UUID
            case 'f__nfws_forts_werkst_':
                workshopValues['ff1aaeb118005d8506af6f56f7e424a4'] = value # Continued by
            case 'f__nfbm_bem_forts_':
                workshopValues['f71d24e2922d3151603ce144c0972f40'] = value # Note
            case 'f__nfzr_zeitraumforts_':
                workshopValues['f865ade60ba332a0a3ab4b77c39af7f4'] = value # Time-Span
            case _:
                print(f'{key} is not a valid field, skipping.')

    # Create Material
    workshop = Entity(api=api, fields=workshopValues, bundle_id='beb03bccbdffdd31567df370303c1e2d')
    api.save(workshop)

    print(f'Created workshop {index}: {workshop.uri} of {len(workshopsTable)}')

    # Write log
    processedRows = processedRows._append({'uuid': workshopValues['fa7c19f4d03d7d15acf588460654bbf2'][0], 'uri': workshop.uri}, ignore_index=True)
    processedRows.to_csv(f'./logs/processedWorkshops.csv', index=False)

    if test:
        exit()

print('finish')
