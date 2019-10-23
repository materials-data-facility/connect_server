from setuptools import setup, find_packages


setup(
    name='mdf_connect_server',
    version='0.7.2',
    packages=find_packages(),
    description='Materials Data Facility Connect Server infrastructure',
    install_requires=[
        "ase>=3.16.2",
        "awscli>=1.16.90",
        "boto3>=1.7.33",
        "citrination-client>=4.1.1",
        "crossrefapi>=1.3.0",
        "Cython>=0.28.5",
        "dfttopif>=1.0.0",
        "flask>=1.0.2",
        "globus-sdk>=1.7.0",
        "gunicorn>=19.9.0",
        "hyperspy>=1.4.1",
        "jsonschema>=2.6.0",
        "mdf-toolbox>=0.5.0",
        "numpy>=1.16.0",
        "pandas>=0.23.0",
        "pif-ingestor>=1.1.1",
        "Pillow>=5.1.0",
        # "pycalphad>=0.7",  # Must be conda installed
        "pymatgen>=2018.1.13",
        "pypif>=2.1.0",
        "pypif-sdk>=2.2.1",
        "python-magic>=0.4.15",
        "pyyaml>=3.12",
        "requests>=2.18.4",
        "scikit-image>=0.14.2",  # For hyperspy
        "xmltodict>=0.11.0"
    ]
)
