from mdf_forge import Forge
from tqdm import tqdm


def generate_stats(raw=False, return_all=False, many_cutoff=100):
    """Generates statistics on datasets in MDF Search.

    Arguments:
        raw (bool): When False, will print stats to stdout and display a progress bar.
                When True, will return a dict of stats and will not display a progress bar.
                Default False.
        return_all (bool): When False or when raw is False, generate summary statistics.
                When True and raw is True, return the dataset source_ids for each category.
                Extremely verbose.
                Default False.
        many_cutoff (int): The number of records required to be considered "many" records.
                Thie value is inclusive.
                Default 100.

    Returns:
        dict: Stats, when raw is True (else these are printed)
            
    """
    mdf = Forge()
    dataset_list = mdf.match_resource_types("dataset").search()

    all_datasets = []
    zero_records = []
    one_record = []
    multiple_records = []
    many_records = []

    for ds in tqdm(dataset_list, disable=raw):
        source_id = ds["mdf"]["source_id"]
        record_count = mdf.match_resource_types("record") \
                          .match_source_names(source_id) \
                          .search(limit=0, info=True)[1]["total_query_matches"]

        all_datasets.append((source_id, record_count))
        if record_count == 0:
            zero_records.append(source_id)
        elif record_count == 1:
            one_record.append(source_id)
        elif record_count > 1:
            multiple_records.append(source_id)
            if record_count >= int(many_cutoff):
                many_records.append(source_id)

    if raw:
        returnable = {}
        returnable["all_datasets_count"] = len(all_datasets)
        returnable["zero_records_count"] = len(zero_records)
        returnable["one_record_count"] = len(one_record)
        returnable["multiple_records_count"] = len(multiple_records)
        returnable["many_records_count"] = len(many_records)
        returnable["one_or_more_count"] = len(one_record) + len(multiple_records)

        if return_all:
            returnable["all_datasets"] = all_datasets
            returnable["zero_records"] = zero_records
            returnable["one_record"] = one_record
            returnable["multiple_records"] = multiple_records
            returnable["many_records"] = many_records
            returnable["one_or_more"] = one_record + multiple_records

        return returnable
    else:
        print("MDF Search Statistics")
        print("---------------------")
        print("Total datasets:", len(all_datasets))
        print("Datasets with zero records:", len(zero_records))
        print("Datasets with any records: ", len(one_record) + len(multiple_records))
        print()
        print("Datasets with exactly one record:   ", len(one_record))
        print("Datasets with more than one record: ", len(multiple_records))
        print("Datasets with more than", many_cutoff, "records:", len(many_records))
        print()
        return


if __name__ == "__main__":
    generate_stats()
