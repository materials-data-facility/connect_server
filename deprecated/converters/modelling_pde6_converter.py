import json
import sys
import os
from tqdm import tqdm
from mdf_forge.toolbox import find_files
from mdf_refinery.parsers.ase_parser import parse_ase
from mdf_refinery.validator import Validator

# VERSION 0.3.0

# This is the converter for: Modeling of the phosphodiesterase (PDE6)
# Arguments:
#   input_path (string): The file or directory where the data resides.
#       NOTE: Do not hard-code the path to the data in the converter (the filename can be hard-coded, though). The converter should be portable.
#   metadata (string or dict): The path to the JSON dataset metadata file, a dict or json.dumps string containing the dataset metadata, or None to specify the metadata here. Default None.
#   verbose (bool): Should the script print status messages to standard output? Default False.
#       NOTE: The converter should have NO output if verbose is False, unless there is an error.
def convert(input_path, metadata=None, verbose=False):
    if verbose:
        print("Begin converting")

    # Collect the metadata
    # NOTE: For fields that represent people (e.g. mdf-data_contact), other IDs can be added (ex. "github": "jgaff").
    #    It is recommended that all people listed in mdf-data_contributor have a github username listed.
    #
    # If there are other useful fields not covered here, another block (dictionary at the same level as "mdf") can be created for those fields.
    # The block must be called the same thing as the source_name for the dataset.
    if not metadata:
        ## Metadata:dataset
        dataset_metadata = {
            "mdf": {

                "title": "Modeling of the phosphodiesterase (PDE6)",
                "acl": ["public"],
                "source_name": "modelling_pde6",

                "data_contact": {
                    
                    "given_name": "Feixia",
                    "family_name": "Chu",
                    "email": "feixia.chu@unh.edu",
                    "institution": "University of New Hampshire",

                },

                "data_contributor": [{
                    
                    "given_name": "Evan",
                    "family_name": "Pike",
                    "email": "dep78@uchicago.edu",
                    "institution": "The University of Chicago",
                    "github": "dep78",

                }],

                "citation": ["Zeng-Elmore, X., Gao, X.-Z., Pellarin, R., Schneidman-Duhovny, D., Zhang, X.-J., Kozacka, K. A., … Chu, F. (2014). Modeling of the phosphodiesterase (PDE6) [Data set]. J Mol Biol. Zenodo. http://doi.org/10.5281/zenodo.46599"],

                "author": [{

                    "given_name": "Xiaohui",
                    "family_name": "Zeng-Elmore",
                    "institution": "University of New Hampshire",

                },
                {

                    "given_name": "Xiong-Zhuo",
                    "family_name": "Gao",
                    "institution": "University of New Hampshire",

                },
                {

                    "given_name": "Riccardo",
                    "family_name": "Pellarin",
                    "institution": "University of California, San Francisco",

                },
                {

                    "given_name": "Dina",
                    "family_name": "Schneidman-Duhovny,",
                    "institution": "University of California, San Francisco",

                },
                {

                    "given_name": "Xiu-Jun",
                    "family_name": "Zhang",
                    "institution": "University of New Hampshire",

                },
                {

                    "given_name": "Katie A.",
                    "family_name": "Kozacka",
                    "institution": "University of New Hampshire",

                },
                {

                    "given_name": "Yang",
                    "family_name": "Tang",
                    "institution": "University of New Hampshire",

                },
                {

                    "given_name": "Andrej",
                    "family_name": "Sali",
                    "institution": "University of California, San Francisco",

                },
                {

                    "given_name": "Robert J.",
                    "family_name": "Chalkley",
                    "institution": "University of California, San Francisco",

                },
                {

                    "given_name": "Rick H.",
                    "family_name": "Cote",
                    "institution": "University of New Hampshire",

                },
                {

                    "given_name": "Feixia",
                    "family_name": "Chu",
                    "email": "feixia.chu@unh.edu",
                    "institution": "University of New Hampshire",

                }],

                "license": "http://www.opensource.org/licenses/LGPL-2.1",
                "collection": "Modelling PDE6",
                "tags": ["Integrative Modeling Platform (IMP)", "Chemical crosslinks", "Electron microscopy density map", "MODELLER"],
                "description": "Photoreceptor phosphodiesterase (PDE6) is the central effector enzyme in visual excitation pathway in rod and cone photoreceptors. Its tight regulation is essential for the speed, sensitivity, recovery and adaptation of visual detection. Although major steps in the PDE6 activation/deactivation pathway have been identified, mechanistic understanding of PDE6 regulation is limited by the lack of knowledge about the molecular organization of the PDE6 holoenzyme (αβγγ). Here, we characterize the PDE6 holoenzyme by integrative structural determination of the PDE6 catalytic dimer (αβ), based primarily on chemical cross-linking and mass spectrometric analysis.",
                "year": 2014,

                "links": {

                    "landing_page": "https://doi.org/10.5281/zenodo.46599",
                    "publication": ["https://github.com/integrativemodeling/pde6/tree/v1.0","https://doi.org/10.1016/j.jmb.2014.07.033"],
                    #"data_doi": "",
                    #"related_id": ,

                    "zip": {

                        #"globus_endpoint": ,
                        "http_host": "https://zenodo.org",

                        "path": "/record/46599/files/pde6-v1.0.zip",
                        },
                    },
                },

            #"mrr": {

                #},

            #"dc": {

                #},


        }
        ## End metadata
    elif type(metadata) is str:
        try:
            dataset_metadata = json.loads(metadata)
        except Exception:
            try:
                with open(metadata, 'r') as metadata_file:
                    dataset_metadata = json.load(metadata_file)
            except Exception as e:
                sys.exit("Error: Unable to read metadata: " + repr(e))
    elif type(metadata) is dict:
        dataset_metadata = metadata
    else:
        sys.exit("Error: Invalid metadata parameter")



    # Make a Validator to help write the feedstock
    # You must pass the metadata to the constructor
    # Each Validator instance can only be used for a single dataset
    # If the metadata is incorrect, the constructor will throw an exception and the program will exit
    dataset_validator = Validator(dataset_metadata)


    # Get the data
    #    Each record should be exactly one dictionary
    #    You must write your records using the Validator one at a time
    #    It is recommended that you use a parser to help with this process if one is available for your datatype
    #    Each record also needs its own metadata
    for data_file in tqdm(find_files(input_path, "pdb"), desc="Processing files", disable=not verbose):
        record = parse_ase(os.path.join(data_file["path"], data_file["filename"]), "proteindatabank")
        ## Metadata:record
        record_metadata = {
            "mdf": {

                "title": "Modelling PDE6 - " + record["chemical_formula"],
                "acl": ["public"],
                "composition": record["chemical_formula"],

#                "tags": ,
#                "description": ,
                #"raw": json.dumps(record),

                "links": {

#                    "landing_page": ,
#                    "publication": ,
#                    "data_doi": ,
#                    "related_id": ,

                    "pdb": {

                        "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                        "http_host": "https://data.materialsdatafacility.org",

                        "path": "/collections/modelling_pde6/" + data_file["no_root_path"] + "/" + data_file["filename"],
                        },
                    },

#                "citation": ,

#                "data_contact": {

#                    "given_name": ,
#                    "family_name": ,
#                    "email": ,
#                    "institution": ,

#                    },

#                "author": [{

#                    "given_name": ,
#                    "family_name": ,
#                    "email": ,
#                    "institution": ,

#                    }],

#                "year": ,

                },

           # "dc": {

           # },


        }
        ## End metadata

        # Pass each individual record to the Validator
        result = dataset_validator.write_record(record_metadata)

        # Check if the Validator accepted the record, and stop processing if it didn't
        # If the Validator returns "success" == True, the record was written successfully
        if not result["success"]:
            if not dataset_validator.cancel_validation()["success"]:
                print("Error cancelling validation. The partial feedstock may not be removed.")
            raise ValueError(result["message"] + "\n" + result.get("details", ""))


    # You're done!
    if verbose:
        print("Finished converting")
