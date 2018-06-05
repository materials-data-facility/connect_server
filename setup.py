from setuptools import setup, find_packages


setup(
    name='mdf_connect',
    version='0.3.1',
    packages=find_packages(),
    description='Materials Data Facility Connect infrastructure',
    install_requires=[
        "ase>=3.16.2",
        "boto3>=1.10.32",
        ("calphad_tdb_ingester @ https://github.com/CitrineInformatics/"
         "calphad-tdb-ingester/archive/develop.zip"),
        "citrination-client>=4.1.1",
        "crossrefapi>=1.3.0",
        ("csv_template_ingester @ https://github.com/CitrineInformatics/"
         "csv_template_ingester/archive/develop.zip"),
        "dfttopif>=1.0.0",
        "flask>=1.0.2",
        "globus-sdk>=1.5.0",
        "hyperspy>=1.3.1",
        "jsonschema>=2.6.0",
        "mdf-toolbox>=0.2.3",
        "pandas>=0.23.0",
        "pif-ingestor @ https://github.com/CitrineInformatics/pif-ingestor/archive/develop.zip",
        "Pillow>=5.1.0",
        "pycalphad>=0.7",
        "pymatgen>=2018.5.22",
        "pymongo>=3.6.1",  # For bson.ObjectId
        "pypif>=2.1.0",
        "pypif-sdk @ https://github.com/CitrineInformatics/pypif-sdk/archive/develop.zip",
        "python-magic>=0.4.15",
        "pyyaml>=3.12",
        "requests>=2.18.4"
    ]
)
