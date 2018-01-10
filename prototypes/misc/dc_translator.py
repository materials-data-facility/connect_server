import json
import os
from tqdm import tqdm
from mdf_toolbox.toolbox import find_files

translate_template = {
##    "mdf.title": "dc.titles,.title",
#$    "mdf.acl": "mdf.acl",
#$    "mdf.source_name": "mdf.source_name",
#    "mdf.data_contact.given_name": ,
#    "mdf.data_contact.family_name": ,
#    "mdf.data_contact.email": ,
##     "mdf.data_contact.institution": ,
#?    "mdf.data_contributor.given_name": ,
#    "mdf.data_contributor.family_name": ,
#    "mdf.data_contributor.email": ,
#    "mdf.data_contributor.institution": ,
#    "mdf.data_contributor.github": ,
#?    "mdf.citation": ,
##    "mdf.author.given_name": ,
##    "mdf.author.family_name": ,
##    "mdf.author.email": ,
##    "mdf.author.institution": ,
##    "mdf.license": "dc.rightsList,.rightsURI",
        #"dc.rightsList.Rights
#    "mdf.repository": ,
#    "mdf.collection": ,
##    "mdf.tags": "dc.subjects,.subject",
##    "mdf.description": ,
#    "mdf.year": ,
#$    "mdf.links.landing_page": ,
##    "mdf.links.publication": ,
##    "mdf.links.data_doi": ,
#    "mdf.links.related_id": ,
#$    "mdf.links.data link.globus_endpoint" : ,
#$    "mdf.links.data link.http_host" : ,
#$    "mdf.links.data link.path" : ,
#    "dc": None,
#    "mrr": None,
    # Hidden
#    "mdf.data_contact.full_name" ,
#    "mdf.data_contributor.full_name": ,
##    "mdf.author.full_name": ,
#$    "mdf.links.parent_id": ,
##    "mdf.ingest_date": "dc.dates,.date",
        #"dc.dates,.dateType": "Created",
#$    "mdf.metadata_version": ,
#$    "mdf.mdf_id": "mdf.mdf_id",
##    "mdf.resource_type": "dc.resourceType.resourceTypeGeneral", # Dataset
        #"dc.resourceType.resourceType": datatype
}

record_fields = [
    "mdf.composition",
    "mdf.raw",
    # Hidden
    "mdf.parent_id",
    "mdf.elements",
    "mdf.scroll_id"
]


def translate_all(input_path, output_path):
    for file_data in tqdm(find_files(input_path, "json$"), desc="Translating"):
        source_name = file_data["filename"].replace("_all", "")
        translated_md = []
        with open(os.path.join(file_data["path"], file_data["filename"]), 'r') as feedstock:
            ds_md = json.loads(feedstock.readline())
            translated_md.append(translate_dataset(ds_md))
            for line in feedstock:
                try:
                    translated_md.append(translate_record(json.loads(line)))
                except json.JSONDecodeError:
                    print("Error on '", source_name, "' line '", line, "'.", sep="")
        with open(os.path.join(output_path, source_name), 'w') as out_file:
            for md in translated_md:
                json.dump(md, out_file)
                out_file.write("\n")
    print("Finished")


def translate_dataset(md):
    # Assemble dc block as new_md
    mdf = md["mdf"]
    new_md = {}
    new_md["identifier"] = {
        "identifier": mdf.get("links", {}).get("data_DOI", ":unav"),
        "identifierType": "DOI"
        }
    # Creators
    creators = []
    for author in mdf.get("author", []):
        creators.append({
            "creatorName": author.get("family_name", "") + ", " + author.get("given_name", ""),
            "givenName": author.get("given_name", ""),
            "familyName": author.get("family_name", ""),
            "affiliations": [author.get("institution", "")]
        })
    new_md["creators"] = creators
    new_md["titles"] = [{
        "title": mdf.get("title", "")
    }]
#    new_md["publisher"] = ""
    new_md["publicationYear"] = str(mdf.get("year", ""))
    new_md["subjects"] = [{"subject": kwd} for kwd in mdf.get("tags", [])]
    new_md["contributors"] = [{
        "contributorName": (mdf.get("data_contact", {}).get("family_name", "")
                            + ", "
                            + mdf.get("data_contact", {}).get("given_name", "")),
        "givenName": mdf.get("data_contact", {}).get("given_name", ""),
        "familyName": mdf.get("data_contact", {}).get("family_name", ""),
        "affiliations": [mdf.get("data_contact", {}).get("institution", "")],
        "contributorType": "ContactPerson"
        }]
    new_md["dates"] = [{
        "date": mdf.get("ingest_date", ""),
        "dateType": "Collected"
        }]
    new_md["language"] = "en-us",
    new_md["resourceType"] = {
        "resourceTypeGeneral": "Dataset" if mdf.get("resource_type", "") == "dataset" else "Other",
        "resourceType": "JSON"
        }
    new_md["relatedIdentifiers"] = [{
        "relatedIdentifier": mdf.get("links", {}).get("publication", ""),
        "relatedIdentifierType": "DOI",
        "relationType": "IsPartOf"
        }]
#    new_md["formats"] = [datatype]
#    new_md["version"] = ""
    new_md["rightsList"] = [{
        "rights": mdf.get("license", ""),
        "rightsURI": mdf.get("license", "")
        }]
    new_md["descriptions"] = [{
        "descriptionType": "Other",
        "description": mdf.get("description", "")
        }]
#    new_md["fundingReferences"] = []

    # Assemble mdf block as mdf_md
    mdf_md = {}
    mdf_md["acl"] = mdf.get("acl", "")
    mdf_md["source_name"] = mdf.get("source_name", "")
    mdf_md["landing_page"] = mdf.get("links", {}).get("landing_page", "")
    mdf_md["parent_id"] = mdf.get("links", {}).get("parent_id", "")
    mdf_md["metadata_version"] = mdf.get("metadata_version", "")
    mdf_md["mdf_id"] = mdf.get("mdf_id", "")
    data_links = []
    for key, value in mdf.get("links", {}).items():
        if type(value) is dict and value.get("path"):
            value["datatype"] = key
            data_links.append(value)
    mdf_md["data_links"] = data_links

    # Clear empty values and assemble final metadata
    final_md = {
        "mdf": {},
        "dc": {}
        }
    # Clean dc block
    for key, value in new_md.items():
        # Certain fields must be checked differently
        if key == "titles":
            if value[0]["title"]:
                final_md["dc"][key] = value
        elif key == "contributors":
            if value[0]["givenName"]:
                final_md["dc"][key] = value
        elif key == "dates":
            if value[0]["date"]:
                final_md["dc"][key] = value
        elif key == "relatedIdentifiers":
            if value[0]["relatedIdentifier"]:
                final_md["dc"][key] = value
        elif key == "rightsList":
            if value[0]["rightsURI"]:
                final_md["dc"][key] = value
        elif key == "descriptions":
            if value[0]["description"]:
                final_md["dc"][key] = value
        elif value:
            final_md["dc"][key] = value
    # Clean mdf block
    for key, value in mdf_md.items():
        if value:
            final_md["mdf"][key] = value

    return final_md


def translate_record(md):
    mdf = md["mdf"]
    source_name = mdf.get("source_name", "source_name")
    # Assemble mdf block
    mdf_md = {}
    mdf_md["acl"] = mdf.get("acl", "")
    mdf_md["source_name"] = mdf.get("source_name", "")
    mdf_md["landing_page"] = mdf.get("links", {}).get("landing_page", "")
    mdf_md["parent_id"] = mdf.get("links", {}).get("parent_id", "")
    mdf_md["mdf_id"] = mdf.get("mdf_id", "")
    mdf_md["scroll_id"] = mdf.get("scroll_id", "")
    data_links = []
    for key, value in mdf.get("links", {}).items():
        if type(value) is dict and value.get("path"):
            value["datatype"] = key
            data_links.append(value)
    mdf_md["data_links"] = data_links

    # Assemble structure block
    structure_md = {}
    structure_md["composition"] = mdf.get("composition", "")
    structure_md["elements"] = mdf.get("elements", "")

    # Assemble source_name block
    sn_md = {}
    sn_md["raw"] = mdf.get("raw", "")
    #resource type

    # Clean blocks
    final_md = {
        "mdf": {},
        "structure": {},
        source_name: {}
        }
    for key, value in mdf_md.items():
        if value:
            final_md["mdf"][key] = value
    for key, value in structure_md.items():
        if value:
            final_md["structure"][key] = value
    for key, value in sn_md.items():
        if value:
            final_md[source_name][key] = value

    return final_md


