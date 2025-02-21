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

# Simple log

try:
    processedRows = pd.read_csv(f'./logs/processedMarks.csv')
except FileNotFoundError:
    processedRows = pd.DataFrame(columns=['id', 'markId', 'uuid', 'uri'])

# Load mark table
markTable = pd.read_sql_table('c__mar', con=engine)
print(f'Processing {len(markTable)} marks...')

# Create mark
for index, row in markTable.iterrows():
    # For every row in table...
    if index < processedRows['id'].max():
        # skip if already processed
        print(f'Skipping already processed mark {row['id']}')
        continue
    # Create Entity property dicts
    markValues = {}
    creationValues = {}
    digitisationProcessValues = {'f32274ec0032b8778ba69d20108590cc': [str(uuid.uuid4())]}
    dimensionValues = {}
    featureValues = {}
    featureDimensionValues = {}
    imageValues = {}
    imageAssignmentValues = {'f067784f5b1ff850672124a2b05360de': [str(uuid.uuid4())]}

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
        # Map columns to fields. We use assignments for reification
        # for nested semantics, because we need to be efficient.
        match key:
            case 'id':
                continue
            case 'f__uuid':
                markValues['fb40b199b4032e55acc152f994e93b45'] = value # UUID
            case 'f__3002_pub_kue_nr_':
                markValues['f6f0572ebec9c98e164d0e9aa0650c2e'] = value # Artist Number
            case 'f__6700_mar_dok_nr_':
                markValues['fe577970c02f173170ff3848a36b3b79'] = value # Mark Document Number
            case 'f__6770_rosenb_nr_':
                markValues['f6fc4b5726c97bad8b03ede860491649'] = value # Rosenberg Number
            case 'f__9990_kommentar':
                markValues['f01e527e707ff36bf966baa01c163378'] = value # Comment
            case 'f__68an_abdruck_nr_':
                markValues['f8324ea3c9ee378f1e19035e092aadb9'] = value  # Print Number
            case 'f__68nk_besonderheiten':
                markValues['fa21e323a8a7a99ce3489e1f7753ac5f'] = value  # Special Features
            case 'f__8470_aufnahmenr_':
                markValues['f67031e2a2b81ad9f318dc5b11d5a6af'] = value  # Recording number
            case 'f__684b_breite_marke':
                # We map dimensions to Dimension entity.
                dimensionValues.setdefault(key, {})['f31e9c7e2de5549daea1790a74615288'] = ['width']  # Type
                dimensionValues[key]['f3f805d270890837a6493e7e60a96487'] = value  # Dimension
                dimensionValues[key]['f802fd7bf45be523a9b188411a591420'] = [str(uuid.uuid4())] # UUID

            case 'f__684h_hoehe_marke':
                dimensionValues.setdefault(key, {})['f31e9c7e2de5549daea1790a74615288'] = ['hight'] # Type
                dimensionValues[key]['f3f805d270890837a6493e7e60a96487'] = value # Dimension
                dimensionValues[key]['f802fd7bf45be523a9b188411a591420'] = [str(uuid.uuid4())] # UUID
            case 'f__68na_bz_breite_hoehe':
                dimensionValues.setdefault(key, {})['f31e9c7e2de5549daea1790a74615288'] = ['width_x_hight'] # Type
                dimensionValues[key]['f3f805d270890837a6493e7e60a96487'] = value # Dimension
                dimensionValues[key]['f802fd7bf45be523a9b188411a591420'] = [str(uuid.uuid4())] # UUID
            case 'f__6840_rahmenform':
                # We map features to Feature entity.
                featureValues.setdefault(key, {})['fdfb3c4f670aa1260924cecd09ca4bbb'] = ['frame_shape']  # Type
                featureValues[key]['fbccee184fa531d58b3b46eb8ac4626f'] = value  # Feature
                featureValues[key]['f299e2a145b508e376f2bf2e44cbe219'] = [str(uuid.uuid4())]  # UUID
            case 'f__684d_darst__marke':
                # We map features to Feature entity.
                featureValues.setdefault(key, {})['fdfb3c4f670aa1260924cecd09ca4bbb'] = ['design']  # Type
                featureValues[key]['fbccee184fa531d58b3b46eb8ac4626f'] = value  # Feature
                featureValues[key]['f299e2a145b508e376f2bf2e44cbe219'] = [str(uuid.uuid4())]  # UUID
            case 'f__684l_text_marke':
                # We map features to Feature entity.
                featureValues.setdefault(key, {})['fdfb3c4f670aa1260924cecd09ca4bbb'] = ['text']  # Type
                featureValues[key]['fbccee184fa531d58b3b46eb8ac4626f'] = value  # Feature
                featureValues[key]['f299e2a145b508e376f2bf2e44cbe219'] = [str(uuid.uuid4())]  # UUID
            case 'f__68nb_randanschluss':
                # We map features to Feature entity.
                featureValues.setdefault(key, {})['fdfb3c4f670aa1260924cecd09ca4bbb'] = ['edge_connection']  # Type
                featureValues[key]['fbccee184fa531d58b3b46eb8ac4626f'] = value  # Feature
                featureValues[key]['f299e2a145b508e376f2bf2e44cbe219'] = [str(uuid.uuid4())]  # UUID
            case 'f__68nc_form_haste':
                # We map features to Feature entity.
                featureValues.setdefault(key, {})['fdfb3c4f670aa1260924cecd09ca4bbb'] = ['haste_mould']  # Type
                featureValues[key]['fbccee184fa531d58b3b46eb8ac4626f'] = value  # Feature
                featureValues[key]['f299e2a145b508e376f2bf2e44cbe219'] = [str(uuid.uuid4())]  # UUID
            case 'f__68nd_form_schraegstr_':
                # We map features to Feature entity.
                featureValues.setdefault(key, {})['fdfb3c4f670aa1260924cecd09ca4bbb'] = ['slash_form_shape']  # Type
                featureValues[key]['fbccee184fa531d58b3b46eb8ac4626f'] = value  # Feature
                featureValues[key]['f299e2a145b508e376f2bf2e44cbe219'] = [str(uuid.uuid4())]  # UUID
            case 'f__68ne_haste_schraegstr_':
                # We map features to Feature entity.
                featureValues.setdefault(key, {})['fdfb3c4f670aa1260924cecd09ca4bbb'] = ['transition_haste_slash']  # Type
                featureValues[key]['fbccee184fa531d58b3b46eb8ac4626f'] = value  # Feature
                featureValues[key]['f299e2a145b508e376f2bf2e44cbe219'] = [str(uuid.uuid4())]  # UUID
            case 'f__68nf_n_knick':
                # We map features to Feature entity.
                featureValues.setdefault(key, {})['fdfb3c4f670aa1260924cecd09ca4bbb'] = ['transition_haste_slash_kink']  # Type
                featureValues[key]['fbccee184fa531d58b3b46eb8ac4626f'] = value  # Feature
                featureValues[key]['f299e2a145b508e376f2bf2e44cbe219'] = [str(uuid.uuid4())]  # UUID
            case 'f__68ng_ueberg__serifen':
                # We map features to Feature entity.
                featureValues.setdefault(key, {})['fdfb3c4f670aa1260924cecd09ca4bbb'] = [
                    'transition_serif_haste']  # Type
                featureValues[key]['fbccee184fa531d58b3b46eb8ac4626f'] = value  # Feature
                featureValues[key]['f299e2a145b508e376f2bf2e44cbe219'] = [str(uuid.uuid4())]  # UUID
            case 'f__68nh_dicke_ser__max_':
                # We map (features) dimensions to Dimension entity.
                featureDimensionValues.setdefault(key, {})['f31e9c7e2de5549daea1790a74615288'] = ['maximum_thickness']  # Type
                featureDimensionValues[key]['f3f805d270890837a6493e7e60a96487'] = value  # Dimension
                featureDimensionValues[key]['f802fd7bf45be523a9b188411a591420'] = [str(uuid.uuid4())]  # UUID
            case 'f__68ni_dicke_ser__min':
                # We map features to Feature entity.
                featureDimensionValues.setdefault(key, {})['f31e9c7e2de5549daea1790a74615288'] = [
                    'minimum_thickness']  # Type
                featureDimensionValues[key]['f3f805d270890837a6493e7e60a96487'] = value  # Dimension
                featureDimensionValues[key]['f802fd7bf45be523a9b188411a591420'] = [str(uuid.uuid4())]  # UUID
            case 'f__68nj_breite_serife':
                # We map features to Feature entity.
                featureDimensionValues.setdefault(key, {})['f31e9c7e2de5549daea1790a74615288'] = [
                    'width']  # Type
                featureDimensionValues[key]['f3f805d270890837a6493e7e60a96487'] = value  # Dimension
                featureDimensionValues[key]['f802fd7bf45be523a9b188411a591420'] = [str(uuid.uuid4())]  # UUID
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
            case 'f__9900_datum_erfassung':
                digitisationProcessValues['f1f5dd22371e5c1de41e0fb099e0e862'] = value  # Recording date
            case 'f__99ae_datum_aenderung':
                digitisationProcessValues['f8976c6a9e5d91fe9caba8a57c27f204'] = value  # Change date
            case 'f__efbm_bem_erfassung':
                digitisationProcessValues['f78a6310d13c717b82ddba814ac59024'] = value # Recording note
            case 'f__ptxt_plug_in_text':
                markValues['ffb8b04e8d57929a596fc32d6a84d07d'] = value # Plugin text
            case _:
                print(f'{key} is not a valid field, skipping.')

    # Create Dimension entities and add their UUIDs to a list
    # because we link Mark and Dimension over the UUID
    dimension = []
    for key, value in dimensionValues.items():
        if value:
            dimensionItem = Entity(api=api, fields=value, bundle_id='b73258adf62f35bd1be3fa2863fab558')
            api.save(dimensionItem)
            dimension.append(value['f802fd7bf45be523a9b188411a591420'][0])

    # Create (feature) Dimension entities and add their UUIDs to a list
    # because we link Feature and its Dimension over the UUID
    featureDimension = []
    for key, value in featureDimensionValues.items():
        if value:
            featureDimensionItem = Entity(api=api, fields=value, bundle_id='b73258adf62f35bd1be3fa2863fab558') # Dimension Bundle
            api.save(featureDimensionItem)
            featureDimension.append(value['f802fd7bf45be523a9b188411a591420'][0])  # Dimension UUID

    # Add the serif feature t the feature list
    if featureDimension:
        featureValues.setdefault('serif', {})['fdfb3c4f670aa1260924cecd09ca4bbb'] = ['serif']  # Feature Type
        featureValues['serif']['f0f825f5d3a6f0e2d67eee311b94cd6f'] = featureDimension  # Dimension UUIDs
        featureValues['serif']['f299e2a145b508e376f2bf2e44cbe219'] = [str(uuid.uuid4())]  # UUID

    # Create Dimension entities and add their UUIDs to a list
    # because we link Mark and Dimension over the UUID
    feature = []
    for key, value in featureValues.items():
        if value:
            featureItem = Entity(api=api, fields=value, bundle_id='b393e1c3db202fbb7a8b54e65eb38227') # Feature Bundle
            api.save(featureItem)
            feature.append(value['f299e2a145b508e376f2bf2e44cbe219'][0]) # Feature UUID

    # Create Image entities and add their UUIDs to a list
    # because we link Image Assignment and Image over the UUID
    imageList = []
    for key, value in imageValues.items():
        if value:
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
    if digitisationProcessValues:
        digitisationProcess = Entity(api=api, fields=digitisationProcessValues, bundle_id='b22e6c47ccb3ab8a974b37279e1bc33b')
        api.save(digitisationProcess)

    # Add the field values for reference
    if dimension:
        markValues['f05807c9d81cd39b814f83de0175d66a'] = dimension # Dimension
    if feature:
        markValues['f3ce49288bc03e9d799f20ea277429db'] = feature  # Feature
    if imageAssignmentValues['f067784f5b1ff850672124a2b05360de'][0]:
        markValues['f73e27498813a922032b18b3f3ab8d10'] = [imageAssignmentValues['f067784f5b1ff850672124a2b05360de'][0]] # Image Assignment
    if digitisationProcessValues['f32274ec0032b8778ba69d20108590cc'][0]:
        markValues['f3baf98f752fc9638de175985183119a'] = [digitisationProcessValues['f32274ec0032b8778ba69d20108590cc'][0]] # Digitisation Process

    # Create Mark
    mark = Entity(api=api, fields=markValues, bundle_id='b2c4e1c984d7758d7c7ec719110f7125')
    api.save(mark)

    print(f'Created mark number {index}: {mark.uri} of {len(markTable)}')

    # Write log
    processedRows = processedRows._append({'id': row['id'], 'markId': markValues['fe577970c02f173170ff3848a36b3b79'][0], 'uuid': markValues['fb40b199b4032e55acc152f994e93b45'][0], 'uri': mark.uri}, ignore_index=True)
    processedRows.to_csv(f'./logs/processedMarks.csv', index=False)

print('finish')
