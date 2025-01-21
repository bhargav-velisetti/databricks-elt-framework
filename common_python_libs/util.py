import json
import os 
import shutil


def validate_ing_format(format : str):

    DBX_ELT_HOME = os.environ.get('DBX_ELT_HOME', '~/databricks-elt-framework' )
    config_file = os.path.join(DBX_ELT_HOME, 'config/ingestion_formats.json')

    with open(config_file) as f:
        data = json.load(f)

        if format in data['formats']:
            print(f'Supported format {format}. Proceed further')
            return True
        else:
            print(f'Not Supported format {format}. Kill the job')
            return False
        
def get_runtime(): 

    DBX_ELT_HOME = os.environ.get('DBX_ELT_HOME', '~/databricks-elt-framework' )
    config_file = os.path.join(DBX_ELT_HOME, 'config/runtime.json')
    
    with open(config_file) as f:
        data = json.load(f)

        return data['engine']
    
def local_move_files(file_path: str, archive_dir: str):
    if not os.path.exists(archive_dir):
        os.makedirs(archive_dir)
    shutil.move(file_path, archive_dir)

def aws_move_files(file_path : str, archive_dir : str):
    pass

def gcp_move_files(file_path : str, archive_dir : str):
    pass

def azure_move_files(file_path : str, archive_dir : str):
    pass
    
def archive_files(runtime,file_path : str, archive_dir : str):

    if runtime == 'local':
        local_move_files(file_path, archive_dir)
    elif runtime == 'aws':
        aws_move_files(file_path, archive_dir)
    elif runtime == 'gcp':
        gcp_move_files(file_path, archive_dir)
    elif runtime == 'azure':
        azure_move_files(file_path, archive_dir)
    else:
        print(f'Runtime {runtime} is not supported yet')
        raise ValueError(f'Runtime {runtime} is not supported yet')



