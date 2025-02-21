import uuid
from initDb import initDb
from dotenv import load_dotenv
import pandas as pd
from models.models import Artifact, Order, Production, DigitisationProcess

# Initialize the database
print('Initializing the database...')
engine, metadata, Session = initDb(True, './schemas/')
if engine == False:
    print('Database initialization failed.')
    exit()

load_dotenv()

session = Session()

processedRows = pd.DataFrame(columns=['objectId', 'uri'])

# Load Tables

artifactsTable = session.query(Artifact).all()
digitasationProcessTable = session.query(DigitisationProcess).all()
orderTable = session.query(Order).all()
productionTable = session.query(Production).all()

for index, row in artifactsTable.iterrows():
    creationValues = {}
    digitisationValues = {'f32274ec0032b8778ba69d20108590cc': [str(uuid.uuid4())]}
    imageValues = {}
    artifactValues = {}
    productionValues = {'feb48c9a7efc444449b4b8defcd6d8bd': [str(uuid.uuid4())]}
    dimensionValues = {}
    for key, value in row.items():
        if (value is None) or (value == ''):
            continue
        if '&' in str(value):
            value = [x.strip() for x in str(value).split('&')]
        else:
            value = [value]
        match key:
            case 'feb48c9a7efc444449b4b8defcd6d8bd':
                artifactValues['feb48c9a7efc444449b4b8defcd6d8bd'] = value # UUID
            case 'f7e2a8a273ab3d577bf5854902550c09':
                artifactValues['f7e2a8a273ab3d577bf5854902550c09'] = value # Document Identifier
            case 'f6e041bd0b16b21596849732c01cb168':
                artifactValues['f6e041bd0b16b21596849732c01cb168'] = value # NGK Number
            case 'f17c199062692310a45953a5a981da83':
                productionValues['f17c199062692310a45953a5a981da83'] = value # Place of Production
            case 'fd06dcc49a29b1a63fa4a789ec17e5c6':
                artifactValues['fd06dcc49a29b1a63fa4a789ec17e5c6'] = value # Title
            case 'f35c9c9b0991729c36acb41645fe81d1':
                artifactValues['f35c9c9b0991729c36acb41645fe81d1'] = value # Status
            case 'f2fd7f8a81d5eb1a20371b9acfd1ab59':
                artifactValues['f2fd7f8a81d5eb1a20371b9acfd1ab59'] = value # Genre
            case 'f05bbd6e29a7d303e4370b04c12b3f75':
                artifactValues['f05bbd6e29a7d303e4370b04c12b3f75'] = value # Formattribute
            case 'f593fa773a6ea458101ba2325a18abbe':
                artifactValues['f593fa773a6ea458101ba2325a18abbe'] = value # artifact type
            case 'f476ba24127d4dff1018acebf45a05f6':
                artifactValues['f476ba24127d4dff1018acebf45a05f6'] = value # Function
            case 'fa7cfd9dbb3d2517c1898b3051d8dbed':
                artifactValues['fa7cfd9dbb3d2517c1898b3051d8dbed'] = value # Shape
            case 'f8309a21fa79bc6bd2506060b419d2df':
                artifactValues['f8309a21fa79bc6bd2506060b419d2df'] = value # Figure
            case 'f3f805d270890837a6493e7e60a96487_hight':
                dimensionValues.setdefault(key, {})['f31e9c7e2de5549daea1790a74615288'] = ['height']  # Type
                dimensionValues[key]['f3f805d270890837a6493e7e60a96487'] = value  # Dimension
            case 'f3f805d270890837a6493e7e60a96487_width':
                dimensionValues.setdefault(key, {})['f31e9c7e2de5549daea1790a74615288'] = ['width'] # Type
                dimensionValues[key]['f3f805d270890837a6493e7e60a96487'] = value # Dimension
            case 'f3f805d270890837a6493e7e60a96487_depth':
                dimensionValues.setdefault(key, {})['f31e9c7e2de5549daea1790a74615288'] = ['depth'] # Type
                dimensionValues[key]['f3f805d270890837a6493e7e60a96487'] = value # Dimension
            case 'f3f805d270890837a6493e7e60a96487_length':
                dimensionValues.setdefault(key, {})['f31e9c7e2de5549daea1790a74615288'] = ['length'] # Type
                dimensionValues[key]['f3f805d270890837a6493e7e60a96487'] = value # Dimension
            case 'f3f805d270890837a6493e7e60a96487_diameter':
                dimensionValues.setdefault(key, {})['f31e9c7e2de5549daea1790a74615288'] = ['diameter'] # Type
                dimensionValues[key]['f3f805d270890837a6493e7e60a96487'] = value # Dimension
            case 'f3f805d270890837a6493e7e60a96487_weigth':
                dimensionValues.setdefault(key, {})['f31e9c7e2de5549daea1790a74615288'] = ['weight'] # Type
                dimensionValues[key]['f3f805d270890837a6493e7e60a96487'] = value # Dimension
            case 'f3f805d270890837a6493e7e60a96487_historial_weight':
                dimensionValues.setdefault(key, {})['f31e9c7e2de5549daea1790a74615288'] = ['historical_weight'] # Type
                dimensionValues[key]['f3f805d270890837a6493e7e60a96487'] = value # Dimension
            case 'f6abbd4f39a6f79de5de2b14b98e51ff':
                artifactValues['f6abbd4f39a6f79de5de2b14b98e51ff'] = value # Keywords
            case 'f26ad2bc1f084478cd7011f7b8451526':
                artifactValues['f26ad2bc1f084478cd7011f7b8451526'] = value # Description
            case 'f40120d7c13ef02b486c69245f6c2306':
                artifactValues['f40120d7c13ef02b486c69245f6c2306'] = value # History
            case 'fd3740649cc06f45677eb0546908cdac':
                artifactValues['fd3740649cc06f45677eb0546908cdac'] = value # Print Number
            case 'feb10344eaa7a5f414d1e8392853eba9':
                for item in value:
                    if item is not None:
                        item = item.replace('Objekte\\', 'objects/')
                        item = item.replace('Objekte3\\', 'objects/')
                        item = item.replace('Objekte4\\', 'objects/')
                        item = item.replace('Objekte5\\', 'objects/')
                        item = item.replace('Marken\\', 'marks/')
                        imageValues.setdefault(item, {})['feb10344eaa7a5f414d1e8392853eba9'] = [item] # Reproduction Number (Image)
                        imageValues[item]['fc8d57e55f203c75c2f8a1ae79378ac7'] = ['public://artifact_images/' + item + '.jpg'] # File
            case 'fee0db94d62fae6370a89ff4757ff539':
                artifactValues['fee0db94d62fae6370a89ff4757ff539'] = value # Catalogue_of_Works
            case 'fefe289aa0c9563a153be6da7d37e3ff':
                artifactValues['fefe289aa0c9563a153be6da7d37e3ff'] = value # Comment
            case 'f1f5dd22371e5c1de41e0fb099e0e862':
                digitisationValues['f1f5dd22371e5c1de41e0fb099e0e862'] = value  # Recording date
            case 'f8976c6a9e5d91fe9caba8a57c27f204':
                digitisationValues['f8976c6a9e5d91fe9caba8a57c27f204'] = value  # Change date
            case 'f78a6310d13c717b82ddba814ac59024':
                digitisationValues['f78a6310d13c717b82ddba814ac59024'] = value # Recording note
            case 'ffb8b04e8d57929a596fc32d6a84d07d':
                artifactValues['ffb8b04e8d57929a596fc32d6a84d07d'] = value # Plugin text
            case _:
                print(f'{key} is not a valid field, skipping.')




    # case 'f__410a_auftraggeber__uuid':
    #     clientValues['fa613fdf8c591a1ece4cb69eb50a2c2c'] = value # Client (Production)
    # case 'f__5007_beziehung__uuid':
    #     artifactValues['f6764d6258963038f1e27d5220a86c65'] = value # artifact assignment
    # case 'f__5064_num__dat___uuid':
    #     productionValues['ff6195ddca1d7c0438b5b6bbe41ffbb5'] = value # Production Date (Production)
    # case 'f__5280_material__uuid':
    #     artifactValues['f6c31c1a85d9e56988a7039e5e13c7fb'] = value # Material
    # case 'f__6760_markenart__uuid':
    #     artifactValues['f45b7c2331355aa88cf4b5af7069d3ba'] = value # Mark
    # case 'f__67b0_bz_dok_nr__uuid':
    #     artifactValues['f67eeff97e9d80bdc39d3d26e0b13bcb'] = value # Assignment
    # case 'f__8130_que_kurzt___uuid':
    #     artifactValues['fc156bf551960773e2717b87737b0dab'] = value # Source
    # case 'f__ref__8330_lit_kurzt__uuid':
    #     artifactValues['fc7c7d372ed19b8ec158e3a76faf1bf6'] = value # Literature
    # case 'f__8490_fotograf__uuid':
    #     creationValues['f4ac419cded0b30ff267b66c69646606'] = value # Creator (Image,Creation)
    # case 'f__ob28_status_verwalt___uuid':
    #     artifactValues['f9d23067d50ec9060904abb1e06db7ab'] = value # Administration
    # case 'f__ob30_bez_kuenstler__uuid':
    #     artifactValues['fa135694408c255d6b8f51d61de1bf3e'] = value # Artist Activity


    production = Entity(api=api, fields=productionValues, bundle_id='be89c84bf729f8e135e2e7b5936f1a44')

    api.save(production)


    dimension = []
    for key, value in dimensionValues.items():
        dimensionItem = Entity(api=api, fields=value, bundle_id='b73258adf62f35bd1be3fa2863fab558')
        api.save(dimensionItem)
        dimension.append(dimensionItem)


    image = []
    for key, value in imageValues.items():
        #creation = Entity(api=api, fields=creationValues, bundle_id='b59f12befc94b313c9389c498eddd6f1')
        #api.save(creation)
        #value['b59f12befc94b313c9389c498eddd6f1'] = [creation]
        imageItem = Entity(api=api, fields=value, bundle_id='b8c6c4b478ead1c80e175ad0f98dafe3')
        api.save(imageItem)
        image.append(imageItem)

    digitisation = Entity(api=api, fields=digitisationValues, bundle_id='b22e6c47ccb3ab8a974b37279e1bc33b')
    api.save(digitisation)

    artifactValues['fb44f0fa6c27e1ee03f6a21288272de2'] = [productionValues['feb48c9a7efc444449b4b8defcd6d8bd'][0]]
    artifactValues['fc700eb3f24f4f2a6c165128aa7117f1'] = dimension
    artifactValues['fc533c36f8b2ea59a6d83f53f3b53083'] = image
    artifactValues['f5a3f90d920da3db4cfdbaa6264b0e89'] = [digitisationValues['f32274ec0032b8778ba69d20108590cc'][0]]

    artifact = Entity(api=api, fields=artifactValues, bundle_id='bd30c2c64a3caa8bb1628c780c3f24bb')
    api.save(artifact)
    print(f'Created artifact {artifact.uri}')
    processedRows = processedRows._append({'objectId': artifactValues['f7e2a8a273ab3d577bf5854902550c09'][0], 'uuid': artifactValues['feb48c9a7efc444449b4b8defcd6d8bd'][0], 'uri': artifact.uri}, ignore_index=True)
    processedRows.to_csv(f'processedRows.csv', index=False)

print('finish')
