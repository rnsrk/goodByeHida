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
    processedRows = pd.read_csv(f'./logs/processedLiteratures.csv')
except FileNotFoundError:
    processedRows = pd.DataFrame(columns=['id', 'literatureId', 'uuid', 'uri'])

# Load sources table
literaturesTable = pd.read_sql_table('c__lit', con=engine)

literatureValues = {}
digitisationProcessValues = {'f32274ec0032b8778ba69d20108590cc': [str(uuid.uuid4())]}

# Create literatures
for index, row in literaturesTable.iterrows():
    # For every row in table...
    if index < len(processedRows) and literaturesTable.iloc[index, 0] == processedRows.iloc[index, 0]:
        # skip if already processed
        print(f'Skipping already processed literature {literaturesTable.iloc[index, 0]}')
        continue
    # Create Entity property dicts
    literatureValues = {}
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
                literatureValues['fd58e0884f7cf63f8436c2789fcd2745'] = value # UUID
            case 'f__9990_kommentar':
                literatureValues['f3208633f7767cc9f5e44e768818df20'] = value # Comment
            case 'f__8270_verfasser':
                literatureValues['f60a88060c75068b4bf2eefd5221793f'] = value # Creator
            case 'f__8324_ersch_jahr':
                literatureValues['fdae7bd743ae58bf623feca3a26bcf6c'] = value # Date
            case 'f__8280_hrsg':
                literatureValues['fd0bc706876adee304892f8f9e34567f'] = value # Editor
            case 'f__8346_signatur':
                literatureValues['fb434c214be21f7e82a851d6524c2850'] = value # Identifier
            case 'f__9970_schlagwort':
                literatureValues['f1a55055944adf5d4e866a1768633a7f'] = value # Keyword
            case 'f__8200_lit_dok_nr_':
                literatureValues['f3bdd54b9ea5808a571200e9c60e103e'] = value # Literature Document Identifier
            case 'f__9971_sw_goldschmied':
                literatureValues['f21a286fec5d48ea238c10877ee2b0db'] = value # Mentioned Actor
            case 'f__8308_bibl_zusatz':
                literatureValues['f1674a743a13a3d74b0c6ebb2cf0043f'] = value # Note
            case 'f__8319_seitenangabe':
                literatureValues['f0d1716a40498f52abd4a6522aa5f3ef'] = value #  Pages
            case 'f__8320_ersch_ort':
                literatureValues['fc3cafc0f542cef2a0e1189873ff58a3'] = value #  Publication Place
            case 'f__8300_serientitel':
                literatureValues['f660f34eb7091c1b0f4b492e49a0e71b'] = value #  Series Title
            case 'f__8330_lit_kurzt_':
                literatureValues['f84416d4380cdd30e8b9fcea57f58957'] = value # Shorttitle
            case 'f__8307_titelzusatz':
                literatureValues['f8521679ac8f6441ddb086f1c5ed7528'] = value # Subtitle
            case 'f__8290_titel':
                literatureValues['fa1ae40cc9940569d5a1e3ea13e33488'] = value # Title
            case 'f__8260_art':
                literatureValues['f92c6453d265a952a56252e7d93cedea'] = value # Type
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
    literatureValues['f59a2ad5cce3e51f172215ea88afac41'] = [digitisationProcessValues['f32274ec0032b8778ba69d20108590cc'][0]] # Digitisation Process

    # Create Material
    literature = Entity(api=api, fields=literatureValues, bundle_id='bafe9c3d3b640d4d1a16b104f367ac91')
    api.save(literature)

    print(f'Created literature {index}: {literature.uri} of {len(literaturesTable)}')

    # Write log
    processedRows = processedRows._append({'id': row['id'], 'literatureId': literatureValues['f3bdd54b9ea5808a571200e9c60e103e'][0], 'uuid': literatureValues['fd58e0884f7cf63f8436c2789fcd2745'][0], 'uri': literature.uri}, ignore_index=True)
    processedRows.to_csv(f'./logs/processedLiteratures.csv', index=False)

print('finish')
