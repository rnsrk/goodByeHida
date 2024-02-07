import json
import os
from sqlalchemy import Column, Integer, String, Table
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

def createClass(name, columns):
    """Create a SQLAlchemy class from a JSON schema.

    Args:
        name (str): The name of the class.
        columns (list): The columns of the class.

    Returns:
        SQLAlchemy.Class: The SQLAlchemy class.
    """
    # Transform name and add prefix
    className = name.lower().replace('-', '_').replace('.', '_').replace(' ', '_').replace('(', '_').replace(')', '_').replace('ä', 'ae').replace('ö', 'oe').replace('ü', 'ue').replace('ß', 'ss').replace('?', '_')
    tableName = name.lower().replace('-', '_').replace('.', '_').replace(' ', '_').replace('(', '_').replace(')', '_').replace('ä', 'ae').replace('ö', 'oe').replace('ü', 'ue').replace('ß', 'ss').replace('?', '_')

    # Transform columns and add prefix
    attrs = {'__tablename__': tableName}
    attrs.update({prop.lower().replace('-', '_').replace('.', '_').replace(' ', '_').replace('(', '_').replace(')','_').replace('ä', 'ae').replace('ö', 'oe').replace('ü', 'ue').replace('ß', 'ss').replace('?', '_'): (Column(String, primary_key=True) if prop.lower() == 'uuid' else Column(String)) for prop in columns})

    # If 'uuid' is not in columns, add 'id' as primary key
    if 'uuid' not in [prop.lower() for prop in columns]:
        attrs['id'] = Column(Integer, primary_key=True)

    # Create SQLAlchemy class
    cls = type(className, (Base,), attrs)

    # Define the table with extend_existing=True
    Table(tableName, Base.metadata, extend_existing=True)

    return cls

def initClassesFromSchemas(schemaDir):
    """Initialize the classes from the schemas.
    """

    if not os.path.exists(schemaDir):
        print('Schema directory does not exist.')
        return False

    schemaList = os.listdir(schemaDir)

    if not schemaList:
        print('No schemas JSON\'s found.')
        return False

    for fileName in schemaList:
        if fileName.endswith('.json'):
            with open(os.path.join(schemaDir, fileName), 'r') as f:
                data = json.load(f)
                cls = createClass(data['name'], data['columns'])
                globals()[cls.__name__] = cls  # Add the class to the global namespace
    print('Classes initialized from schemas.')
    return True