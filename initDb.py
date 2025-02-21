import dotenv
import os
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
from initSchemas import initClassesFromSchemas, Base

# Load the environment variables
dotenv.load_dotenv()

# Database Initialization
def initDb(_production, schemaDir):
    """Initialize the database.
    """

    # Initialize the classes from the schemas
    print('Initializing the classes from the schemas...')
    if not initClassesFromSchemas(schemaDir):
        print('Cannot initialize database. No schemas found.')
        return (False, False)

    if _production:
        dbName = 'ngk'
    else:
        dbName = 'testngk'

    # Get the directory of the script
    dirPath = os.path.dirname(os.path.realpath(__file__))

    # Create the path of the database file
    dbPath = os.path.join(dirPath, dbName)

    engine = create_engine(f'mysql+pymysql://{os.getenv("DB_USER")}:{os.getenv("DB_PASS")}@{os.getenv("DB_HOST")}/{os.getenv("DB_NAME")}', echo=False)
    metadata = MetaData()
    Session = sessionmaker(bind=engine)

    # Create all tables in the engine
    Base.metadata.create_all(engine)

    print('Database initialized.')
    return engine, metadata, Session
