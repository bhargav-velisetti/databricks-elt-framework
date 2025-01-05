from setuptools import setup, find_packages

setup(
    name='databricks-elt-framework',
    version='0.1.0',
    description='A framework for ELT processes using Databricks and External Systems',
    author='Bhargav Velisetti',
    author_email='bhargav.v14@gmail.com',
    url='https://github.com/bhargav-velisetti/databricks-elt-framework',
    packages=['common_pyspark_libs', 'common_python_libs', 'ingestion', 'etl-engineering'],
    install_requires=[
        'pyspark>=3.1.1',
        'delta-spark>=1.0.0',
        'kafka-python>=2.0.2'
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: Apache License Version 2.0',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.9',
)