from google.cloud import bigquery

def get_bq_schema_from_df(df):
    schema = []
    for column in df.columns:
        dtype = df[column].dtype.name  
        
        if dtype == 'object':
            field_type = 'STRING'
        elif dtype.startswith('int'):
            field_type = 'INTEGER'
        elif dtype.startswith('float'):
            field_type = 'FLOAT'
        elif dtype == 'bool':
            field_type = 'BOOLEAN'
        elif dtype.startswith('datetime'):
            field_type = 'TIMESTAMP'
        else:
            field_type = 'STRING'  

        schema.append(bigquery.SchemaField(column, field_type, mode='NULLABLE'))
    
    return schema