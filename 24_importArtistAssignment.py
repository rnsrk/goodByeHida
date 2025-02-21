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
    processedRows = pd.read_csv(f'./logs/processedArtistAssignment.csv')
except FileNotFoundError:
    processedRows = pd.DataFrame(columns=['docId', 'uuid', 'uri'])

# Load sources table
artistRelationsTable = pd.read_sql_table('c__ob30_bez_kuenstler', con=engine)

artistRelationValues = {}

# Create artistRelations
for index, row in artistRelationsTable.iterrows():
    # For every row in table...
    if index < len(processedRows) and artistRelationsTable.loc[index, 'id'] == processedRows.loc[index, 'docId']:
        # skip if already processed
        print(f'Skipping already processed artistRelation {artistRelationsTable.loc[index, 'id']}')
        continue
    # Create Entity property dicts
    for key, value in row.items():
        print('value: ', value)

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
                artistRelationValues['fc150259d31fea8a3f992e7beb901fa4'] = value # UUID
            case 'f__3100_name':
                artistRelationValues['ff5bf58133f9351d03e2ee92b6f8bb7e'] = value # Artist Name
            case 'f__3475_ber__funkt_':
                artistRelationValues['fc0c7d8c6b736489210bc42ef0f1406a'] = value # Occupation
            case 'f__ob30_bez_kuenstler':
                artistRelationValues['f575d4f2c8ea5d37618cea708c2a7c5e'] = value # Relation
            case _:
                print(f'{key} is not a valid field, skipping.')


    artistRelation = Entity(api=api, fields=artistRelationValues, bundle_id='bc8826cc7d9c9373ce71cfc0251c2a4f')
    api.save(artistRelation)

    print(f'Created artistRelation {index}: {artistRelation.uri} of {len(artistRelationsTable)}')

    # Write log
    processedRows = processedRows._append({'docId': docId, 'uuid': artistRelationValues['fc150259d31fea8a3f992e7beb901fa4'][0], 'uri': artistRelation.uri}, ignore_index=True)
    processedRows.to_csv(f'./logs/processedArtistAssignment.csv', index=False)

print('finish')
