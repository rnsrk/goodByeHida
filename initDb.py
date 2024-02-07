import os
from sqlalchemy import create_engine, MetaData
from initSchemas import initClassesFromSchemas, Base



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
        dbName = 'database.db'
    else:
        dbName = 'test.db'

    # Get the directory of the script
    dirPath = os.path.dirname(os.path.realpath(__file__))

    # Create the path of the database file
    dbPath = os.path.join(dirPath, dbName)

    engine = create_engine(f'sqlite:///{dbPath}')
    metadata = MetaData()

    # Create all tables in the engine
    Base.metadata.create_all(engine)

    print('Database initialized.')
    return engine, metadata
