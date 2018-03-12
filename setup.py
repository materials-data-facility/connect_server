from setuptools import setup, find_packages


setup(
    name='mdf_connect',
    version='0.3.0',
    packages=find_packages(),
    description='Materials Data Facility Connect infrastructure',
    install_requires=[
        "ase>=3.15.0",
        "boto3>=1.5.22",
        "citrination-client>=3.1.0",
        "crossrefapi>=1.2.0",
        "flask>=0.12.2",
        "globus-sdk>=1.4.1",
        "jsonschema>=2.6.0",
        "mdf-toolbox>=0.1.5",
        "pandas>=0.20.3",
        "pif-ingestor",
        "Pillow>=3.1.2",
        "pymatgen>=2017.10.16",
        "pymongo>=3.5.1",  # For bson.ObjectId
        "pypif>=2.1.0",
        "pypif-sdk",
        "python-magic>=0.4.13",
        "requests>=2.18.4"
    ]
)
