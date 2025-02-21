import pandas as pd
from initDb import initDb

processedRows = pd.read_csv(f'processedRows.csv')
for indexRow, row in processedRows.iterrows():
    for indexCol, value in row.items():
            processedRows.loc[indexRow, indexCol] = value.replace('[', '').replace(']', '').replace('\'', '')


print('Initializing the database...')
engine, metadata = initDb(True, './schemas/')
objects = pd.read_sql_table('c__obj', con=engine)
processedRows = pd.merge(processedRows, objects[['f__5000_obj_dok_nr_', 'f__uuid']].rename(columns={'f__uuid': 'uuid'}), how='left', left_on='objectId', right_on='f__5000_obj_dok_nr_')
processedRows = processedRows.drop('f__5000_obj_dok_nr_', axis=1)

processedRows.to_csv(f'cleanedProcessedRows.csv', index=False)
print('finish')
