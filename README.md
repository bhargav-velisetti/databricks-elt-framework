# Project Compatability Matrix for Local Setup 

| SW | VERSION REQUIRED |
| --- |  --- |
|  OS |  Windows 10 |
| PYTHON VERSION | >=3.9 |
| JAVA VERSION | >=8 |
| SCALA VERSION | 2.12 |
| SPARK VERSION | 3.1.1 |
| KAFKA VERSION | 3.1.0 |
| DELTA STORAGE VERSION | 1.0.1 |

## Directory structure
```bash
databricks-elt-framework/
├── common_pyspark_libs/
│   ├── __init__.py
│   ├── some_pyspark_utility.py
├── common_python_libs/
│   ├── __init__.py
│   ├── some_python_utility.py
├── ingestion/
│   ├── __init__.py
│   ├── kafka_consumer.py
├── etl_engineering/
│   ├── __init__.py
│   └── main.py
├── setup.py
└── README.md
```
### Build and install your package

```bash
export DBX_ELT_HOME="~/databricks-elt-framework"
cd $DBX_ELT_HOME
python setup.py sdist bdist_wheel
pip install .
```

