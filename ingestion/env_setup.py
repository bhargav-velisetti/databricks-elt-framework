
import sys
import os

def add_modules_path():
        DBX_ELT_HOME = os.environ.get('DBX_ELT_HOME', '~/databricks-elt-framework' )
        sys.path.insert(0, DBX_ELT_HOME)