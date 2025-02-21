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
    processedRows = pd.read_csv(f'./logs/processedAdministrators.csv')
except FileNotFoundError:
    processedRows = pd.DataFrame(columns=['administratorId', 'uuid', 'uri'])

# Load sources table
administratorsTable = pd.read_sql_table('c__vwr', con=engine)

administratorValues = {}
digitisationProcessValues = {'f32274ec0032b8778ba69d20108590cc': [str(uuid.uuid4())]}

# Create administrators
for index, row in administratorsTable.iterrows():
    # For every row in table...
    if index < len(processedRows) and administratorsTable.iloc[index, 0] == processedRows.iloc[index, 0]:
        # skip if already processed
        print(f'Skipping already processed administrator {administratorsTable.iloc[index, 0]}')
        continue
    # Create Entity property dicts
    administratorValues = {}
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
                administratorValues['f707e595ce7301d61c064e8e44c9c4f4'] = value # UUID
            case 'f__vwra_vwr_adresse':
                administratorValues['f303bbabf3d97536777b0f552d20bc7a'] = value # Address
            case 'f__vwrn_vwr_dok_nr_':
                administratorValues['f37e82c36b4fc6b275a1a86a389481e1'] = value # Administrator document number
            case 'f__vwrb_verw_publ_bez':
                administratorValues['ffc50ffbcc3f411ed63e3c6dfc6b4d80'] = value # Appellation in publication
            case 'f__9990_kommentar':
                administratorValues['fcf9600af8c3eff355eb42466e9aac39'] = value # Comment
            case 'f__2900_verw_langbez_':
                administratorValues['f78d3c9e6800adbb8a9af0867cbdf3c7'] = value # Long Appellation
            case 'f__2864_ort':
                administratorValues['fecf6c9d7cbae513923e411178516378'] = value # Place
            case 'f__290a_verw_kurzbez_':
                administratorValues['fddaae99f4c6a835d9f9f195523c85f7'] = value # Short appellation
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
    administratorValues['f3ec4640a87bd4534763af0fca050193'] = [digitisationProcessValues['f32274ec0032b8778ba69d20108590cc'][0]] # Digitisation Process

    # Create Material
    administrator = Entity(api=api, fields=administratorValues, bundle_id='b4e5a6a31ff575ab09b07b5f27d322ab') # Administrator
    api.save(administrator)

    print(f'Created administrator {index}: {administrator.uri}')

    # Write log
    processedRows = processedRows._append({'administratorId': administratorValues['f37e82c36b4fc6b275a1a86a389481e1'][0], 'uuid': administratorValues['f707e595ce7301d61c064e8e44c9c4f4'][0], 'uri': administrator.uri}, ignore_index=True)
    processedRows.to_csv(f'./logs/processedAdministrators.csv', index=False)

print('finish')
