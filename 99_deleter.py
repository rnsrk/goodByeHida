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


processedRows = pd.read_csv(f'./logs/delete.csv')


# Create entities
for index, row in processedRows.iterrows():
    try:
        entity = api.get_entity(row['uri'])
        entity.delete()
        print('delete ' + row['uri'] + ' ' + str(index) + ' of ' + str(len(processedRows)))
    except:
        print('could not delete ' + row['uri'] + ' ' + str(index) + ' of ' + str(len(processedRows)))
print('finish')
