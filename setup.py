from setuptools import setup, find_packages


setup(
    name='mdf_connect',
    version='0.3.0',
    packages=find_packages(),
    description='Materials Data Facility Connect infrastructure',
    install_requires=[
        "ase>=3.15.0",
        "boto3>=1.5.30",
        "citrination-client>=3.2.0",
        "crossrefapi>=1.2.0",
        "dfttopif>=1.0.0",
        "flask>=0.12.2",
        "globus-sdk>=1.5.0",
        "hyperspy>=1.3",
        "jsonschema>=2.6.0",
        "mdf-toolbox>=0.2.3",
        "pandas>=0.22.0",
        "pif-ingestor",
        "Pillow>=5.0.0",
        "pymatgen>=2018.2.13",
        "pymongo>=3.5.1",  # For bson.ObjectId
        "pypif>=2.1.0",
        "pypif-sdk",
        "python-magic>=0.4.15",
        "pyyaml>=3.12",
        "requests>=2.18.4"
    ]
)
