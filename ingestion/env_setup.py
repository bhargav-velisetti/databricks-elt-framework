
import sys
import os
from pathlib import Path


def add_modules_path():
        path = os.path.dirname(Path(__file__).parent.absolute())
        DBX_ELT_HOME = os.environ.get('DBX_ELT_HOME', path )
        sys.path.insert(0, DBX_ELT_HOME)
        #return path 