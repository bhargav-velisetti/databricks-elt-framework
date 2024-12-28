import json
import os 


def validate_ing_format(format : str):

    DBX_ELT_HOME = os.environ.get('DBX_ELT_HOME', '~/databricks-elt-framework' )
    config_file = os.path.join(DBX_ELT_HOME, 'config/ingestion_formats.json')

    with open(config_file) as f:
        data = json.load(f)

        if format in data['formats']:
            print(f'Supported format {format}. Proceed further')
        else:
            raise ValueError(f'Not supported format {format}')
        
def get_runtime(): 

    DBX_ELT_HOME = os.environ.get('DBX_ELT_HOME', '~/databricks-elt-framework' )
    config_file = os.path.join(DBX_ELT_HOME, 'config/runtime.json')
    
    with open(config_file) as f:
        data = json.load(f)

        return data['engine']


