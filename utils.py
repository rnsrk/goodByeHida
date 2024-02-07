from sqlalchemy import MetaData, Table

def cleanEntityName(entityName):
  return entityName.lower().replace('-', '_').replace('.', '_').replace(' ', '_').replace('(', '_').replace(')', '_').replace('ä', 'ae').replace('ö', 'oe').replace('ü', 'ue').replace('ß', 'ss').replace('?', '_')

def tableExists(engine, table_name):
  metadata = MetaData()
  metadata.reflect(bind=engine)
  return table_name in metadata.tables