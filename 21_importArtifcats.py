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
    processedRows = pd.read_csv(f'./logs/processedArtifacts.csv')
except FileNotFoundError:
    processedRows = pd.DataFrame(columns=['artifactId', 'uuid', 'uri'])

# Load artifacts table
artifactsTable = pd.read_sql_table('c__obj', con=engine)

# Create artifacts
for index, row in artifactsTable.iterrows():
    # For every row in table...
    if index < len(processedRows) and artifactsTable.iloc[index, 0] == processedRows.iloc[index, 0]:
        # skip if already processed
        print(f'Skipping already processed artifact {artifactsTable.iloc[index, 0]}')
        continue
    # Create Entity property dicts
    artifactValues = {}
    creationValues = {}
    digitisationProcessValues = {'f32274ec0032b8778ba69d20108590cc': [str(uuid.uuid4())]}
    imageValues = {}
    imageAssignmentValues = {'f067784f5b1ff850672124a2b05360de': [str(uuid.uuid4())]}
    productionPlaceAssignmentValues = {'f40cc95db3ccaa1dbbf27294338d9f07': [str(uuid.uuid4())]}
    dimensionValues = {}
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
                artifactValues['feb48c9a7efc444449b4b8defcd6d8bd'] = value # UUID
            case 'f__5000_obj_dok_nr_':
                artifactValues['f7e2a8a273ab3d577bf5854902550c09'] = value # Document Identifier
                docId = value[0]
            case 'f__500n_ngk_nr_':
                artifactValues['f6e041bd0b16b21596849732c01cb168'] = value # NGK Number
            case 'f__5130_entst_ort':
                # We map productions place to Production Place Assignment entity.
                productionPlaceAssignmentValues['f43f9589eef324fb12c26226dfe94246'] = value # Production Place
            case 'f__5200_obj_titel':
                artifactValues['fd06dcc49a29b1a63fa4a789ec17e5c6'] = value # Title
            case 'f__5210_status':
                artifactValues['f35c9c9b0991729c36acb41645fe81d1'] = value # Status
            case 'f__5220_gattung':
                artifactValues['f2fd7f8a81d5eb1a20371b9acfd1ab59'] = value # Genre
            case 'f__5223_form__attribut':
                artifactValues['f05bbd6e29a7d303e4370b04c12b3f75'] = value # Formattribute
            case 'f__5226_art':
                artifactValues['f593fa773a6ea458101ba2325a18abbe'] = value # artifact type
            case 'f__523f_funktion':
                artifactValues['f476ba24127d4dff1018acebf45a05f6'] = value # Function
            case 'f__5240_formtyp':
                artifactValues['fa7cfd9dbb3d2517c1898b3051d8dbed'] = value # Shape
            case 'f__524g_gestalt':
                artifactValues['f8309a21fa79bc6bd2506060b419d2df'] = value # Figure
            case 'f__5362_hoehe':
                # We map dimensions to Dimension entity.
                dimensionValues.setdefault(key, {})['f31e9c7e2de5549daea1790a74615288'] = ['height']  # Type
                dimensionValues[key]['f3f805d270890837a6493e7e60a96487'] = value  # Dimension
                dimensionValues[key]['f802fd7bf45be523a9b188411a591420'] = [str(uuid.uuid4())] # UUID

            case 'f__5364_breite':
                dimensionValues.setdefault(key, {})['f31e9c7e2de5549daea1790a74615288'] = ['width'] # Type
                dimensionValues[key]['f3f805d270890837a6493e7e60a96487'] = value # Dimension
                dimensionValues[key]['f802fd7bf45be523a9b188411a591420'] = [str(uuid.uuid4())] # UUID

            case 'f__5366_tiefe':
                dimensionValues.setdefault(key, {})['f31e9c7e2de5549daea1790a74615288'] = ['depth'] # Type
                dimensionValues[key]['f3f805d270890837a6493e7e60a96487'] = value # Dimension
                dimensionValues[key]['f802fd7bf45be523a9b188411a591420'] = [str(uuid.uuid4())] # UUID

            case 'f__5368_laenge':
                dimensionValues.setdefault(key, {})['f31e9c7e2de5549daea1790a74615288'] = ['length'] # Type
                dimensionValues[key]['f3f805d270890837a6493e7e60a96487'] = value # Dimension
                dimensionValues[key]['f802fd7bf45be523a9b188411a591420'] = [str(uuid.uuid4())] # UUID

            case 'f__5370_durchmesser':
                dimensionValues.setdefault(key, {})['f31e9c7e2de5549daea1790a74615288'] = ['diameter'] # Type
                dimensionValues[key]['f3f805d270890837a6493e7e60a96487'] = value # Dimension
                dimensionValues[key]['f802fd7bf45be523a9b188411a591420'] = [str(uuid.uuid4())] # UUID

            case 'f__5380_gewicht':
                dimensionValues.setdefault(key, {})['f31e9c7e2de5549daea1790a74615288'] = ['weight'] # Type
                dimensionValues[key]['f3f805d270890837a6493e7e60a96487'] = value # Dimension
                dimensionValues[key]['f802fd7bf45be523a9b188411a591420'] = [str(uuid.uuid4())] # UUID
            case 'f__538h_hist__gewicht':
                dimensionValues.setdefault(key, {})['f31e9c7e2de5549daea1790a74615288'] = ['historical_weight'] # Type
                dimensionValues[key]['f3f805d270890837a6493e7e60a96487'] = value # Dimension
                dimensionValues[key]['f802fd7bf45be523a9b188411a591420'] = [str(uuid.uuid4())] # UUID
            case 'f__55ng_darst__schlagw_':
                artifactValues['f6abbd4f39a6f79de5de2b14b98e51ff'] = value # Keywords
            case 'f__5bes_beschreibung':
                artifactValues['f26ad2bc1f084478cd7011f7b8451526'] = value # Description
            case 'f__5ges_geschichte':
                artifactValues['f40120d7c13ef02b486c69245f6c2306'] = value # History
            case 'f__68an_abdruck_nr_':
                artifactValues['fd3740649cc06f45677eb0546908cdac'] = value # Print Number
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
                        imageValues.setdefault(item, {})['feb10344eaa7a5f414d1e8392853eba9'] = [item] # Reproduction Number (Image)
                        imageValues[item]['fc8d57e55f203c75c2f8a1ae79378ac7'] = ['public://artifact_images/' + item + '.jpg'] # File
                        imageValues[item]['f11beac4b638016479e6f3fbc7e55d1a'] = [str(uuid.uuid4())] # UUID
            case 'f__stwv_statwerkverz':
                artifactValues['fee0db94d62fae6370a89ff4757ff539'] = value # Catalogue_of_Works
            case 'f__9990_kommentar':
                artifactValues['fefe289aa0c9563a153be6da7d37e3ff'] = value # Comment
            case 'f__9900_datum_erfassung':
                digitisationProcessValues['f1f5dd22371e5c1de41e0fb099e0e862'] = value  # Recording date
            case 'f__99ae_datum_aenderung':
                digitisationProcessValues['f8976c6a9e5d91fe9caba8a57c27f204'] = value  # Change date
            case 'f__efbm_bem_erfassung':
                digitisationProcessValues['f78a6310d13c717b82ddba814ac59024'] = value # Recording note
            case 'f__ptxt_plug_in_text':
                artifactValues['ffb8b04e8d57929a596fc32d6a84d07d'] = value # Plugin text
            case _:
                print(f'{key} is not a valid field, skipping.')

    # Create Production Place Assignment
    productionPlaceAssignment = Entity(api=api, fields=productionPlaceAssignmentValues, bundle_id='b13bc6dc04d4bbdafb9536987eb43244')
    api.save(productionPlaceAssignment) # Kai says, we can save all entities at once, but I save it instantly


    # Create Dimension entities and add their UUIDs to a list
    # because we link Artifact and Dimension over the UUID
    dimension = []
    for key, value in dimensionValues.items():
        dimensionItem = Entity(api=api, fields=value, bundle_id='b73258adf62f35bd1be3fa2863fab558')
        api.save(dimensionItem)
        dimension.append(value['f802fd7bf45be523a9b188411a591420'][0])

    # Create Image entities and add their UUIDs to a list
    # because we link Image Assignment and Image over the UUID
    imageList = []
    for key, value in imageValues.items():
        imageItem = Entity(api=api, fields=value, bundle_id='b8c6c4b478ead1c80e175ad0f98dafe3')
        api.save(imageItem)
        imageList.append(value['f11beac4b638016479e6f3fbc7e55d1a'][0])

    # Create Image Assignment entities and add their UUIDs to a list
    # because we link Artifact and Image Assignment over the UUID
    if imageList:
        imageAssignmentValues['f70afb79b45472fee3d02f011caa4b36'] = imageList # List of Image UUIDs
        imageAssignment = Entity(api=api, fields=imageAssignmentValues, bundle_id='b88e5d94fb2a83d62df99cf64d6c010c')
        api.save(imageAssignment)

    # Create Digitisation Process
    digitisationProcess = Entity(api=api, fields=digitisationProcessValues, bundle_id='b22e6c47ccb3ab8a974b37279e1bc33b')
    api.save(digitisationProcess)

    # Add the field values for reference
    # UWAGA! Is the Value Production Place Assignment Correct? UWAGA!
    artifactValues['f2676a0fb8db6ab62235328ae7c7a4b3'] = [productionPlaceAssignmentValues['f40cc95db3ccaa1dbbf27294338d9f07'][0]]  # Production Place Assignment
    artifactValues['fc700eb3f24f4f2a6c165128aa7117f1'] = dimension # Dimension
    artifactValues['f7af1cd9c77448281dd7ecf29ba57e3e'] = [imageAssignmentValues['f067784f5b1ff850672124a2b05360de'][0]] # Image Assignment
    artifactValues['f5a3f90d920da3db4cfdbaa6264b0e89'] = [digitisationProcessValues['f32274ec0032b8778ba69d20108590cc'][0]] # Digitisation Process

    # Create Artifact
    artifact = Entity(api=api, fields=artifactValues, bundle_id='bd30c2c64a3caa8bb1628c780c3f24bb')
    api.save(artifact)

    print(f'Created artifact {index}: {artifact.uri} of {len(artifactsTable)}')

    # Write log
    processedRows = processedRows._append({'artifactId': artifactValues['f7e2a8a273ab3d577bf5854902550c09'][0], 'uuid': artifactValues['feb48c9a7efc444449b4b8defcd6d8bd'][0], 'uri': artifact.uri}, ignore_index=True)
    processedRows.to_csv(f'./logs/processedArtifacts.csv', index=False)

print('finish')
