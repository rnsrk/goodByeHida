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
    processedRows = pd.read_csv(f'./logs/processedArtists.csv')
except FileNotFoundError:
    processedRows = pd.DataFrame(columns=['artistId', 'uuid', 'uri'])

# Load sources table
artistsTable = pd.read_sql_table('c__kue', con=engine)

artistValues = {}
digitisationProcessValues = {'f32274ec0032b8778ba69d20108590cc': [str(uuid.uuid4())]}
imageValues = {}
imageAssignmentValues = {'f067784f5b1ff850672124a2b05360de': [str(uuid.uuid4())]}

# Create artists
for index, row in artistsTable.iterrows():
    # For every row in table...
    if index < len(processedRows) and artistsTable.loc[index, 'f__3000_kue_dok_nr_'] == processedRows.loc[index, 'artistId']:
        # skip if already processed
        print(f'Skipping already processed artist {artistsTable.loc[index, "f__3000_kue_dok_nr_"]}')
        continue
    # Create Entity property dicts
    artistValues = {}
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
                artistValues['fff2eb2283e4cd8df3783602a1bc96ab'] = value # UUID
            case 'f__3170_and__taetigkeit':
                artistValues['f01f51e385e5f206653e029ff5c845c4'] = value # Alternate occupation
            case 'f__3000_kue_dok_nr_':
                artistValues['f61deac361ac5e0731edbf214761d15c'] = value # Artist Document Number
            case 'f__3002_pub_kue_nr_':
                artistValues['f46b2ec14ce05d2618427c526198d64e'] = value # Artist published number
            case 'f__9990_kommentar':
                artistValues['fedc08e4225ac800e5d9f16bf345d181'] = value # Comment
            case 'f__3360_letzte_erw_':
                artistValues['f1419788b918f4c4a13393fd09ff37b3'] = value # Last Mentioned
            case 'f__6700_mar_dok_nr_':
                artistValues['f3d63eec34c00556cbadf635f78d815a'] = value # Mark Assignment
            case 'f__33gs_meister_als':
                artistValues['f30b60be791fb13f919c31510ca4de50'] = value # Master Education
            case 'f__33mj_meisterjahr':
                artistValues['fd2d07bb9ea1eadacdf28e41cacb92c1'] = value # Master Year
            case 'f__3100_name':
                artistValues['f71c047dad23083850a13d489386bf31'] = value # Name
            case 'f__3105_abw_schreibw_':
                artistValues['fbe84024bf9fad8f6a545b3af75d8b1b'] = value # Name Variants
            case 'f__3166_fakt__taetig_als':
                artistValues['fb0373e9fd949984cf9c09ec1ea0746c'] = value # Occupation
            case 'f__336p_1__posth__erw_':
                artistValues['fe079424bb6196d4a9721f84c43361f8'] = value # Posthumous Mentioned
            case 'f__8540_repro_nr_':
                # We map images to Image entity
                for item in value:
                    if item is not None:
                        # Replace dir paths in name
                        item = item.replace('Objekte\\', 'objects/')
                        item = item.replace('Objekte3\\', 'objects/')
                        item = item.replace('Objekte4\\', 'objects/')
                        item = item.replace('Objekte5\\', 'objects/')
                        item = item.replace('objekte5\\', 'objects/')
                        item = item.replace('Marken\\', 'marks/')
                        item = item.replace('Marken/', 'marks/')
                        item = item.replace('MArken\\', 'marks/')
                        item = item.replace('Goldschmiede/', 'goldsmiths/')
                        item = item.replace('Goldschmiede\\', 'goldsmiths/')
                        item = item.replace('Epitaphien/', 'epitaphies/')
                        item = item.replace('Epitaphien\\', 'epitaphies/')
                        imageValues.setdefault(item, {})['feb10344eaa7a5f414d1e8392853eba9'] = [item] # Reproduction Number (Image)
                        imageValues[item]['fc8d57e55f203c75c2f8a1ae79378ac7'] = ['public://artifact_images/' + item + '.jpg'] # File
                        imageValues[item]['f11beac4b638016479e6f3fbc7e55d1a'] = [str(uuid.uuid4())] # UUID
            case 'f__6770_rosenb_nr_':
                artistValues['f82ed1dc96df9230e28e04fef0ff2305'] = value # Rosenberg number
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

    # Create Image entities and add their UUIDs to a list
    # because we link Image Assignment and Image over the UUID
    imageList = []
    for key, value in imageValues.items():
        if value:
            imageItem = Entity(api=api, fields=value, bundle_id='b8c6c4b478ead1c80e175ad0f98dafe3')
            api.save(imageItem)
            imageList.append(value['f11beac4b638016479e6f3fbc7e55d1a'][0])

    # Create Image Assignment entities and add their UUIDs to a list
    # because we link Artist and Image Assignment over the UUID
    if imageList:
        imageAssignmentValues['f70afb79b45472fee3d02f011caa4b36'] = imageList # List of Image UUIDs
        imageAssignment = Entity(api=api, fields=imageAssignmentValues, bundle_id='b88e5d94fb2a83d62df99cf64d6c010c')
        api.save(imageAssignment)

    if imageAssignmentValues['f067784f5b1ff850672124a2b05360de'][0]:
        artistValues['fbcc1a8aa38d416e580e0d1c9ff11e58'] = [imageAssignmentValues['f067784f5b1ff850672124a2b05360de'][0]] # Image Assignment
    if digitisationProcessValues['f32274ec0032b8778ba69d20108590cc'][0]:
        artistValues['f6c2b79f1ba142bb62f83b2c4d805e49'] = [digitisationProcessValues['f32274ec0032b8778ba69d20108590cc'][0]] # Digitisation Process


    # Create Material
    artist = Entity(api=api, fields=artistValues, bundle_id='bc322be33491dacc600dd43fdee09a5c')
    api.save(artist)

    print(f'Created artist {index}: {artist.uri} of {len(artistsTable)}')

    # Write log
    processedRows = processedRows._append({'artistId': artistValues['f61deac361ac5e0731edbf214761d15c'][0], 'uuid': artistValues['fff2eb2283e4cd8df3783602a1bc96ab'][0], 'uri': artist.uri}, ignore_index=True)
    processedRows.to_csv(f'./logs/processedArtists.csv', index=False)

print('finish')
