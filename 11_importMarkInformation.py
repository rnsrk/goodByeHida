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


tableName = "c__6760_markenart"
bundleId = 'bc7ce6906f78e760f22ff13226b1332d' # Mark information assignment

try:
    processedRows = pd.read_csv(f'./logs/processed-{tableName}.csv')
except FileNotFoundError:
    processedRows = pd.DataFrame(columns=['id', 'docId', 'uuid', 'uri'])

# Load sources table
sqlTable = pd.read_sql_table(tableName, con=engine)

entityValues = {}

# Create entities
for index, row in sqlTable.iterrows():
    # For every row in table...
    if index < len(processedRows) and sqlTable.iloc[index, 0] == processedRows.iloc[index, 0]:
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
                continue
            case 'f__uuid':
                entityValues['f3b8aaf7e79229b4da8214d491e375ec'] = value # UUID
                fUuid = value[0]
            case 'f__5064_num__dat_':
                entityValues['fe6921098808e68cae68f0858411826c'] = value # Artist Assignment
            case 'f__6894_anbr_ort':
                entityValues['f694ed57271ab7be57249e0ee5c41ba4'] = value # Location
            case 'f__6700_mar_dok_nr_':
                entityValues['fdd3380d4a11654f32687429796cabc3'] = value # Mark Document Number
            case 'f__6760_markenart':
                entityValues['fd381aa9c3ebdf417e6cbccd60ede279'] = value # Mark Type
            case 'f__684c_bedeutung_bz':
                entityValues['f4947de52885f517baef0cdf3cb53b61'] = value # Meaning Inspection Mark
            case 'f__684a_bedeutung_mz':
                entityValues['f542c4c945725c6fdc5ab6409a877f02'] = value # Meaning Master Mark
            case 'f__6770_rosenb_nr_':
                entityValues['f0ff7020a9c25ea2706875837fe61b04'] = value # Rosenberg Number

            case _:
                print(f'{key} is not a valid field, skipping.')

    # Create Material
    entity = Entity(api=api, fields=entityValues, bundle_id=bundleId)
    api.save(entity)

    print(f'Created entity {index}: {entity.uri} of {len(sqlTable)}')

    # Write log
    processedRows = processedRows._append({'id': row['id'], 'docId': docId, 'uuid': fUuid, 'uri': entity.uri}, ignore_index=True)
    processedRows.to_csv(f'./logs/processed-{tableName}.csv', index=False)

print('finish')
