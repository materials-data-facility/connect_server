import requests
import json
import xmltodict
import os
import os.path
from tqdm import tqdm

from mdf_refinery.config import PATH_DATASETS

url = "http://mrr.materialsdatafacility.org/oai_pmh/server/"
prefixes = [
    "oai_database",
    "oai_datacol",
    "oai_dataset"
    ]

# Harvest MDF MRR OAI-PMH
def harvest(out_dir=PATH_DATASETS, base_url=url, metadata_prefixes=prefixes, resource_types=[], verbose=False):
    #Fetch list of records
    records = []
    for prefix in metadata_prefixes:
        record_res = requests.get(base_url + "?verb=ListRecords&metadataPrefix=" + prefix)
        if record_res.status_code != 200:
            exit("Records GET failure: " + str(record_res.status_code) + " error")
        result = xmltodict.parse(record_res.content)
        try:
            list_records = result["OAI-PMH"]["ListRecords"]
            new_records = list_records if isinstance(list_records, list) else [list_records]
            print(len(new_records))
            records.extend(new_records)
            print(len(records))
        except KeyError: #No results
            if verbose:
                print("No results for", prefix)

    count = 0
    with open(os.path.join(out_dir, "mdf_mrr.json"), 'w') as feed_file:
        for meta_record in tqdm(records, desc="Processing records", disable= not verbose):
            meta_record2 = meta_record["record"]
            if not isinstance(meta_record2, list):
                meta_record2 = [meta_record2]
            for record in meta_record2:
                if ((not resource_types or record["header"]["setSpec"] in resource_types)
                    and not record["header"].get("@status", "") == "deleted"):
                    #Only grab what is desired
                    try:
                        md = record["metadata"]
                    except:
                        print(type(record))
                        print(record["header"]["@status"])
                        return None
    #                  print(len(record))
    #                   resource_num = record["header"]["identifier"].rsplit("/", 1)[1] #identifier is `URL/id_num`
                    # Add mdf data
    #                    record["mdf"] = {
    #                        "acl": ["public"],
    #                        "source_name": "mdf_mrr",
    #                        "links": {
    #                            "landing_page": "http://mrr.materialsdatafacility.org/#" + str(count)
    #                            }
    #                        }
                    json.dump(md, feed_file)
                    feed_file.write("\n")
                    count += 1

    if verbose:
        print("Finished")


