from setuptools import setup, find_packages


setup(
    name='mdf_connect_server',
    version='0.5.1',
    packages=find_packages(),
    description='Materials Data Facility Connect Server infrastructure',
    install_requires=[
        "ase>=3.16.2",
        "awscli>=1.16.24",
        "boto3>=1.7.33",
        "citrination-client>=4.1.1",
        "crossrefapi>=1.3.0",
        "Cython>=0.28.3",
        "dfttopif>=1.0.0",
        "flask>=1.0.2",
        "globus-sdk>=1.5.0",
        "gunicorn>=19.9.0",
        "hyperspy>=1.3.1",
        "jsonschema>=2.6.0",
        "mdf-toolbox>=0.3.2",
        "numpy>=1.14.5"
        "pandas>=0.23.0",
        "pif-ingestor>=1.1.0",
        "Pillow>=5.1.0",
        # "pycalphad>=0.7",  # Must be conda installed
        "pymatgen>=2018.5.22",
        "pymongo>=3.6.1",  # For bson.ObjectId
        "pypif>=2.1.0",
        "pypif-sdk>=2.2.1",
        "python-magic>=0.4.15",
        "pyyaml>=3.12",
        "requests>=2.18.4",
        "xmltodict>=0.11.0"
    ]
)
