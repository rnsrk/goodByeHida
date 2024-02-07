import os
import json
from utils import cleanEntityName
import xml.etree.ElementTree as ET
import shutil
from tqdm import tqdm


def processNode(node, schemaDir: str, parentName: str = None) -> set:
    """ Process a node.

    Args:
        node (Element): The node to process.
        schemaDir (str): The path to the directory to store the schemas.
        parentName (str, optional): The name of the parent node. Defaults to None.

    Returns:
        set: The set of columns.
    """

    if node.tag == "block":
        # If the node is a block, it is the root node.
        key_lbl: str = cleanEntityName(f"{node.get('txt')}")  # The name of the column.
        columns: set = set([])  # The set of columns.
    else:
        # If the node is not a block, it is a child node.
        key_lbl: str = cleanEntityName(f"{node.get('key')}_{node.get('lbl')}")  # The name of the column.
        columns: set = {f"f__{key_lbl}"}  # The set of columns with its own name, cause it has children.
    for child in node:
        if len(child) > 0:
            # If the child node has children, process the child node.
            processNode(child, schemaDir, key_lbl)  # The columns of the child node.
        elif 'txt' in child.attrib:
            # If the child node has a text attribute, we need no column.
            createRelTable(schemaDir, key_lbl, cleanEntityName(f"{child.get('key')}_{child.get('lbl')}"))
            childName: str = cleanEntityName(f"{child.get('key')}_{child.get('lbl')}")  # The name of the child column.

            childColumns = set([f"f__uuid", f"f__{childName}"])
            filePathEntity: str = os.path.join(schemaDir, f"c__{childName}.json")
            if os.path.exists(filePathEntity):
                # If the entity file exists, load the existing columns.
                with open(filePathEntity, 'r', encoding='utf-8') as f:
                    # Load the existing columns from the entity file.
                    existingChildColumns: list = json.load(f).get("columns", [])
                    childColumns.update(existingChildColumns)
            with open(filePathEntity, 'w', encoding='utf-8') as f:
                # Open the entity file to write.

                # Write the entity file with the columns.
                json.dump({"name": f"c__{childName}", "columns": list(childColumns)}, f, ensure_ascii=False)
        else:
            # Iterate through the children of the node.
            childName: str = cleanEntityName(f"{child.get('key')}_{child.get('lbl')}")  # The name of the child column.
            # Add the child column to the set of columns.
            columns.add(f"f__{childName}")

    if columns and len(node) > 0:
        # Check if the node has children

        columnsList: list = sorted(list(columns))  # Sorted list of the columns.

        # Add the uuid column to the list of columns
        columnsList.append("f__uuid")
        filePathEntity: str = os.path.join(schemaDir, f"c__{key_lbl}.json")  # The path to the entity file.
    if os.path.exists(filePathEntity):
        # If the entity file exists, load the existing columns.
        with open(filePathEntity, 'r', encoding='utf-8') as f:
            # Load the existing columns from the entity file.
            existingColumns: list = json.load(f).get("columns", [])  # The existing columns.

            # Add the existing columns to the list of columns.
            columnsList.extend(existingColumns)

            # Remove duplicates
            columnsList = sorted(list(set(columnsList)))

    with open(filePathEntity, 'w', encoding='utf-8') as f:
        # Open the entity file to write.

        # Write the entity file with the columns.
        json.dump({"name": f"c__{key_lbl}", "columns": columnsList}, f, ensure_ascii=False)
    if parentName:
        # If the node has a parent, create a relationship table.
        createRelTable(schemaDir, parentName, key_lbl)
    return columns


def processXmlFile(filePath, schemaDir):
    """Process an XML file.

    Args:
        filePath (str): The path to the XML file.
        schemaDir (str): The path to the directory to store the schemas.
    """
    tree = ET.parse(filePath)  # The XML tree.
    root = tree.getroot()  # The root of the XML tree.

    os.makedirs(schemaDir, exist_ok=True)

    for block in root.iter('block'):
        # Iterate through the blocks in the XML file and process each block.
        if 'txt' in block.attrib:
            # If the block has a text attribute, process the block.
            columns: set = processNode(block, schemaDir)  # The columns of the block.
            columnsList: list = sorted(list(columns))  # Sorted list of the columns.
            filePath: str = os.path.join(schemaDir, f"c__{block.get('txt')}.json")  # The path to the file.

            if os.path.exists(filePath):
                # If the file exists, load the existing columns.
                with open(filePath, 'r', encoding='utf-8') as f:
                    # Load the existing columns from the file.
                    existingColumns: list = json.load(f).get("columns", [])  # The existing columns.

                    # Add the existing columns to the list of columns.
                    columnsList.extend(existingColumns)

                    # Remove duplicates from the list of columns.
                    columnsList = sorted(list(set(columnsList)))

            with open(filePath, 'w', encoding='utf-8') as f:
                # Open the file to write.

                # Write the file with the columns.
                json.dump({"name": f"c__{block.get('txt')}", "columns": columnsList}, f, ensure_ascii=False)


def buildSchemas(dirPath, schemaDir):
    """Parse schemas from XML files and saves them as json.

    Args:
        dirPath (str): The path to the directory containing the XML files.
        schemaDir (str): The path to the directory to store the schemas.
    """
    if os.path.exists(schemaDir):
        # Remove the existing schema directory
        shutil.rmtree(schemaDir)

    # Get the total number of XML files
    totalFiles = sum([len([f for f in files if f.endswith('.xml')]) for r, d, files in os.walk(dirPath)])

    with tqdm(total=totalFiles, desc="Processing XML files", ncols=75) as pbar:
        for dirpath, dirnames, filenames in os.walk(dirPath):
            # Walk through the directory and process each XML file
            for fileName in filenames:
                if fileName.endswith('.xml'):
                    processXmlFile(os.path.join(dirpath, fileName), schemaDir)
                    # Update the progress bar
                    pbar.update(1)
    print('Schemas built.')


def createRelTable(schemaDir: str, parentName: str, key_lbl: str):
    """Create a relationship table.
        Args:
            schemaDir (str): The path to the directory to store the schemas.
            parentName (str): The name of the parent node.
            key_lbl (str): The name of the column.
    """
    tableName = f"r__{parentName}__{key_lbl}"
    filePathRelTable: str = os.path.join(schemaDir,
                                         f"{tableName}.json"
                                         )  # The path to the relationship table file.

    with open(filePathRelTable, 'w', encoding='utf-8') as f:
        # Open the relationship table file to write.

        # Write the relationship table file with the columns.
        json.dump(
            {"name": tableName, "columns": [f"f__{parentName}__uuid", f"f__{key_lbl}__uuid"]},
            f, ensure_ascii=False)
