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
    processedRows = pd.read_csv(f'./logs/processedAdministratorStatus.csv')
except FileNotFoundError:
    processedRows = pd.DataFrame(columns=['id', 'uuid', 'uri'])

# Load sources table
administratorStatusTable = pd.read_sql_table('c__ob28_status_verwalt_', con=engine)

administratorStatusValues = {}

# Create administratorStatuss
for index, row in administratorStatusTable.iterrows():
    # For every row in table...
    if index < len(processedRows) and administratorStatusTable.iloc[index, 0] == processedRows.iloc[index, 0]:
        # skip if already processed
        print(f'Skipping already processed administratorStatus {administratorStatusTable.iloc[index, 0]}')
        continue
    # Create Entity property dicts
    administratorStatusValues = {}
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
                continue
            case 'f__uuid':
                administratorStatusValues['f5ea2a7495ec872781ddc06f862b4270'] = value # UUID
            case 'f__290a_verw_kurzbez_':
                administratorStatusValues['f08562a866d00cd5245c380c20e4e7f9'] = value # Admistrator short appellation
            case 'f__2950_invent_nr_':
                administratorStatusValues['f92ac041f6098335bf4075942a771ee3'] = value # Inventary
            case 'f__2952_alte_i_nr_':
                administratorStatusValues['fdc070143457df491f18347ac97b0f24'] = value # Old Identifier
            case 'f__2864_ort':
                administratorStatusValues['f9bc3796ceff9a3581bd8047545628b9'] = value # Place
            case 'f__ob28_status_verwalt_':
                administratorStatusValues['ff0265deb26c28f139345b89577b2539'] = value # Status
            case 'f__2996_gelt_dauer':
                administratorStatusValues['f3363962b4eaa4d38358bc1d2bda1a7f'] = value # Time-Span
            case _:
                print(f'{key} is not a valid field, skipping.')

    # Create Material
    administratorStatus = Entity(api=api, fields=administratorStatusValues, bundle_id='b45447146729190da3a1d3e19165a6f8')
    api.save(administratorStatus)

    print(f'Created administratorStatus {index}: {administratorStatus.uri} of {len(administratorStatusTable)}')

    # Write log
    processedRows = processedRows._append({'id': row['id'], 'uuid': administratorStatusValues['f5ea2a7495ec872781ddc06f862b4270'][0], 'uri': administratorStatus.uri}, ignore_index=True)
    processedRows.to_csv(f'./logs/processedAdministratorStatus.csv', index=False)

print('finish')
