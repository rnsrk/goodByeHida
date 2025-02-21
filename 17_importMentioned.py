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
tableName = "c__7060_erwaehnt__datum_"
bundleId = 'b04b1756b09ba3260de278824332ad6c'

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
                entityValues['fac8bbc9701f5da711a6a49beca1b3e4'] = value # UUID
                fUuid = value[0]
            case 'f__410a_auftraggeber':
                entityValues['f6b456466f45f72952a953bf169a47cc'] = value # Client
            case 'f__7060_erwaehnt__datum_':
                entityValues['ffdae7d7aeb84467faebf5468fb8b94f'] = value # Date
            case 'f__7100_art_ereignis':
                entityValues['fb462fbc544045fc244da8d490ed1cfc'] = value # Event type
            case 'f__70ls_lit__stelle':
                entityValues['f11f8bc3fdbedc686430ef57edfcf620'] = value # Literature Reference
            case 'f__70lt_lit__kurztitel':
                entityValues['f4ed2a340720f643bcc49ac9581b1181'] = value # Literature short title
            case 'f__34ms_bei_meister_':
                entityValues['f9d8ac79df3eb667db8fb8b23e52a816'] = value # Master
            case 'f__70bm_bem_ereignis':
                entityValues['f37dbed94d03576c91fff9c3c9026da5'] = value # Note
            case 'f__70qs_quelle_stelle':
                entityValues['ffc72e8058fd9efd4bb92270520942bd'] = value # Source reference
            case 'f__70qt_quelle_kurztitel':
                entityValues['f433afdf58621b6962dea8821cf21bb9'] = value # Source short title
            case 'f__3420_taet_ort':
                entityValues['f53e436b293c82f07fb17dd40c01f868'] = value # Workplace
            case _:
                print(f'{key} is not a valid field, skipping.')

    # Create Material
    entity = Entity(api=api, fields=entityValues, bundle_id=bundleId)
    api.save(entity)

    print(f'Created entity {index}: {entity.uri} of {len(tableName)}')

    # Write log
    processedRows = processedRows._append({'docId': docId, 'uuid': fUuid, 'uri': entity.uri}, ignore_index=True)
    processedRows.to_csv(f'./logs/processed-{tableName}.csv', index=False)
    if test:
        exit()

print('finish')
