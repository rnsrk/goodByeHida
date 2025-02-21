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
    processedRows = pd.read_csv(f'./logs/processedSources.csv')
except FileNotFoundError:
    processedRows = pd.DataFrame(columns=['id','sourceId', 'uuid', 'uri'])

# Load sources table
sourcesTable = pd.read_sql_table('c__que', con=engine)

sourceValues = {}
digitisationProcessValues = {'f32274ec0032b8778ba69d20108590cc': [str(uuid.uuid4())]}

# Create sources
for index, row in sourcesTable.iterrows():
    # For every row in table...
    if index < processedRows['id'].max():
        # skip if already processed
        print(f'Skipping already processed source {row['id']}')
        continue
    # Create Entity property dicts
    sourceValues = {}
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
                sourceValues['f9f02815a5631a85948d4d258a455f49'] = value # UUID
            case 'f__9990_kommentar':
                sourceValues['f89a563b07f965ca2dcb0b1bd178e863'] = value # Comment
            case 'f__8080_verfasser':
                sourceValues['f2d2934a6c72b5552f01042338ff5d67'] = value # Creator
            case 'f__80bs_que__beschr_':
                sourceValues['fd2122de6bcd62c61fcb7a9223baa20f'] = value # Description
            case 'f__80bw_que__bewertung':
                sourceValues['f70a7818de6e31eacea22148c92737ac'] = value # Evalutation
            case 'f__8182_transkr__extern':
                sourceValues['f409a3ea352d6bc55c27f6a93d239191'] = value # External Transkript
            case 'f__2950_invent_nr_':
                sourceValues['f71605f258ceb37ee5fcf2cd7871de2c'] = value # Inventary number
            case 'f__2900_verw_langbez_':
                sourceValues['f19d275cd6f48ef64d104997ca99291d'] = value # Long appellation administrator
            case 'f__8540_repro_nr_':
                sourceValues['f881dd5566725dc26a8b25cfba181792'] = value # Reproduction Number
            case 'f__290a_verw_kurzbez_':
                sourceValues['f343d954f8d95f1da98201a7f29ac81f'] = value # Short appellation Administrator
            case 'f__8130_que_kurzt_':
                sourceValues['f3faea3691516939fc4b0c2149ee2e5b'] = value # Shorttitle
            case 'f__8000_que_dok_nr_':
                sourceValues['f50ad6021b42c094f7e551faec831802'] = value # Source Document Identifier
            case 'f__8092_untertitel':
                sourceValues['fb734bd50628353b7b5c0bfc88f2cbdc'] = value # Subtitle
            case 'f__80fp_vorhanden_als':
                sourceValues['fd7b99a3db6191382401d69710ac192f'] = value # There as
            case 'f__8090_titel':
                sourceValues['f399332f583d268f07200efd1e3bb3c5'] = value # Title
            case 'f__8180_transkript_':
                sourceValues['f6585008a698902f45dc2a79b9a3a9de'] = value # Transcript
            case 'f__8060_art':
                sourceValues['f38c664e4f9b2effc83ebc50e1244442'] = value # Type
            case 'f__2990_verbleib':
                sourceValues['fae3bc551d146652898782f712f95749'] = value # Whereabouts
            # Digitisation Process
            case 'f__9900_datum_erfassung':
                digitisationProcessValues['f1f5dd22371e5c1de41e0fb099e0e862'] = value  # Recording date
            case 'f__99ae_datum_aenderung':
                digitisationProcessValues['f8976c6a9e5d91fe9caba8a57c27f204'] = value  # Change date
            case 'f__efbm_bem_erfassung':
                digitisationProcessValues['f78a6310d13c717b82ddba814ac59024'] = value # Recording note
            case _:
                print(f'{key} is not a valid field, skipping.')

    # Create Digitisation Process
    digitisationProcess = Entity(api=api, fields=digitisationProcessValues, bundle_id='b22e6c47ccb3ab8a974b37279e1bc33b')
    api.save(digitisationProcess)

    # Set Digitisation Process
    sourceValues['ffdf27e75013fa55d31f728ff5166f06'] = [digitisationProcessValues['f32274ec0032b8778ba69d20108590cc'][0]] # Digitisation Process

    # Create Material
    source = Entity(api=api, fields=sourceValues, bundle_id='b7dc57a93e008a58514b0d4ca26147b1')
    api.save(source)

    print(f'Created source {index}: {source.uri}')

    # Write log
    processedRows = processedRows._append({'id': row['id'], 'sourceId': sourceValues['f50ad6021b42c094f7e551faec831802'][0], 'uuid': sourceValues['f9f02815a5631a85948d4d258a455f49'][0], 'uri': source.uri}, ignore_index=True)
    processedRows.to_csv(f'./logs/processedSources.csv', index=False)

print('finish')
