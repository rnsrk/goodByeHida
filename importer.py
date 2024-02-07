import os
import xml.etree.ElementTree as ET
import pandas as pd
import uuid
from utils import cleanEntityName, tableExists
from sqlalchemy.orm import Session
from tqdm import tqdm


def insertData2Db(engine: Session, tableName: str, columns: dict):
    """Inserts data into a database table.

  Args:
      engine (): The database engine to use.
      tableName (str): The name of the table to insert the data into.
      columns (dict): A list of dictionaries containing the data to insert.
  """

    if not tableExists(engine, tableName):
        # If the table does not exist, print an error message and return.
        print(f'Table {tableName} does not exist.')
        return

    # Create a dataframe from the columns.
    df = pd.DataFrame([columns])  # The dataframe to insert.

    # Insert the dataframe into the database.
    df.to_sql(tableName, engine, if_exists='append', index=False)


class Importer:
    def __init__(self, engine: Session, metadata: Session, docsDir: str):
        self.engine = engine
        self.metadata = metadata
        self.docsDir = docsDir

    def importNode(self, node: ET.Element, parentUuid: str = None, parentKey: str = None):
        """Imports a node from an XML file into the database.

        Args:
            node (ET.Element): The node to import.
            parentUuid (str, optional): The UUID of the parent node. Defaults to None.
            parentKey (str, optional): The key of the parent node. Defaults to None.

        Returns:
            Dict[Dict]: The data from the node.
        """

        data: dict[dict] = {'f__uuid': parentUuid} if parentUuid else {}  # The data table to import

        # Iterate through the children of the node.
        for child in node:
            # Iterate through the children of the node.

            classKey: str = f"{child.get('key')}_{cleanEntityName(child.get('lbl'))}"  # The key for the class
            className: str = f"c__{classKey}"  # The name of the class
            fieldKey: str = f"{classKey}"  # The key for the field
            fieldName: str = f"f__{fieldKey}"  # The name of the field
            entityUuid: str = str(uuid.uuid4())  # The UUID for the entity

            childData: dict[str, str] = {
                "f__uuid": entityUuid,
            }  # The data table (with uuid) for the child node

            if 'txt' in child.attrib:
                # If the child node has a text attribute, it is an entity.
                childData.update({fieldName: child.get('txt')})

                if len(child) > 0:
                    # If the child node has children, import the data of the child node and its children.

                    # Recursively import the data of the child node.
                    childData.update(self.importNode(child))

                # Insert the data of the child node into the database.
                insertData2Db(self.engine, className, childData)

                # Insert the relationship data into the database.
                self.insertRelData(parentUuid, parentKey, entityUuid, classKey)

            else:
                # If the child node has no children, import the data of the child node.

                key: str = f"f__{child.get('key')}_{cleanEntityName(child.get('lbl'))}"  # The key for the row

                if child.text is not None:
                    row: dict = {key: child.text.replace('###{new_line}### ', '\n')}  # The row to insert

                    data.update(row)
        return data

    def processXmlFile(self, filePath: str, fileName: str):
        """Processes an XML file and imports the data into the database.

      Args:
          filePath (str): The path to the XML file.
          fileName (str): The name of the XML file.
      """

        tree = ET.parse(filePath)  # The XML tree.
        root = tree.getroot()  # The root of the XML tree.

        for block in root.iter('block'):
            # Iterate through the blocks in the XML file and import the data of each block.
            if 'txt' in block.attrib:
                # If the block has a 'txt' attribute, import the data of the block.
                classKey: str = f"{block.get('txt')}"  # The key for the class
                blockUuid: str = str(uuid.uuid4())  # The UUID for the block
                data: dict[dict] = self.importNode(block, blockUuid, classKey)  # The data to import.
                tableName: str = f"c__{cleanEntityName(block.get('txt'))}"  # The name of the table to import the data into.
                try:
                    insertData2Db(self.engine, tableName, data)
                except Exception as e:
                    print(f"An error occurred while inserting data into {tableName}: {e}")

    def importData(self):
        """Imports all XML files in a directory into the database.
            Walks through the directory and processes each XML file.
        """

        # Get the total number of XML files
        totalFiles = sum([len([f for f in files if f.endswith('.xml')]) for r, d, files in os.walk(self.docsDir)])        # Create a progress bar
        with tqdm(total=totalFiles, desc="Processing XML files", ncols=75) as pbar:
            for dirpath, dirnames, filenames in os.walk(self.docsDir):
                # Walk through the directory and process each XML file
                for fileName in filenames:
                    if fileName.endswith('.xml'):
                        self.processXmlFile(os.path.join(dirpath, fileName), fileName)
                        # Update the progress bar
                        pbar.update(1)
        print('Data imported.')

    def insertRelData(self, parentUuid: str, parentKey: str, entityUuid: str, classKey: str):
        """Imports the relationship data into the database.
            Args:
                parentUuid (str): The UUID of the parent entity.
                parentKey (str): The key of the parent entity.
                entityUuid (str): The UUID of the entity.
                classKey (str): The key of the entity.
        """

        relationTableName: str = f"r__{parentKey}__{classKey}"  # The name of the relation table
        relRow = {f"f__{parentKey}__uuid": parentUuid,
                  f"f__{classKey}__uuid": entityUuid}  # The row to insert into the relation table
        relDf = pd.DataFrame([relRow])  # The dataframe to insert into the relation table
        relDf.to_sql(relationTableName, self.engine, if_exists='append',
                     index=False)  # Insert the dataframe into the relation table