import json
import os
from copy import deepcopy
import mdf_toolbox
import re
import logging

logger = logging.getLogger(__name__)


class SourceIDManager:
    # Stopwords to delete from the source_name
    # Not using NTLK to avoid an entire package dependency for one minor feature,
    # and the NLTK stopwords are unlikely to be in a dataset title ("your", "that'll", etc.)
    delete_words = [
        "a",
        "an",
        "and",
        "as",
        "data",
        "dataset",
        "for",
        "from",
        "in",
        "of",
        "or",
        "study",
        "test",  # Clears test flag from new source_id
        "that",
        "the",
        "this",
        "to",
        "very",
        "with"
    ]

    def __init__(self):
        pass

    def split_source_id(self, source_id):
        """Retrieve the source_name and version information from a source_id.
        Not complex logic, but easier to have in one location.
        Standard form: {source_name}_v{search_version}.{submission_version}

        Arguments:
        source_id (str): The source_id to split. If this is not a valid-form source_id,
                         the entire string will be assumed to be the source_name and source_id
                         and the versions will be 0.

        Returns:
        dict:
            success (bool): True if the versions were extracted, False otherwise.
            source_name (str): The base source_name.
            source_id (str): The assembled source_id.
            search_version (int): The Search version from the source_id.
            submission_version (int): The Connect version from the source_id.
        """
        # Check if source_id is valid
        if not re.search("_v[0-9]+\\.[0-9]+$", source_id):
            return {
                "success": False,
                "source_name": source_id,
                "source_id": source_id,
                "search_version": 0,
                "submission_version": 0
            }

        source_name, versions = source_id.rsplit("_v", 1)
        v_info = versions.split(".", 1)
        search_version, submission_version = v_info

        return {
            "success": True,
            "source_name": source_name,
            "source_id": "{}_v{}.{}".format(source_name, search_version, submission_version),
            "search_version": int(search_version),
            "submission_version": int(submission_version)
        }

    def make_source_name(self, title, author, is_test):

        def clean_string(value):
            tokens = [t for t in value.strip().replace("_", " ").split() if t]
            value_clean = []

            for token in tokens:
                # Clean token is lowercase and alphanumeric
                clean_token = "".join([char for
                                       char in token.lower()
                                       if char.isalnum() and char.isascii()])
                if clean_token and clean_token not in self.delete_words:
                    value_clean.append(clean_token)
            return value_clean

        # Clean title tokens
        title_clean = clean_string(title)
        author_word = clean_string(author)

        # Remove author_word from title, if exists (e.g. from previous make_source_id())
        while author_word in title_clean:
            title_clean.remove(author_word)

        # Select words from title for source_name
        # Use up to the first two words + last word
        if len(title_clean) >= 1:
            word1 = title_clean[0]
        else:
            # Must have at least one word
            raise ValueError("Title '{}' invalid: Must have at least one word that is not "
                             "the author name (the following words do not count: '{}')"
                             .format(title, self.delete_words))
        if len(title_clean) >= 2:
            word2 = title_clean[1]
        else:
            word2 = ""
        if len(title_clean) >= 3:
            word3 = title_clean[-1]
        else:
            word3 = ""

        source_name = "{}_{}_{}_{}".format(author_word, word1, word2, word3).strip("_")

        # Add test flag if necessary
        if is_test:
            source_name = "_test_" + source_name

        return source_name

    # SourceID is the primary key - we could make this a uuid
    # Could be datasetID? is in Dyanamo DB and Search records
    # There will be lots of places where you do string.split('_v') to get version
    def make_source_id(self, title, author, index, is_test, sanitize_only=False):
        """Make a source name out of a title."""
        # Remove any existing version number from title
        title = self.split_source_id(title)["source_name"]

        # Tokenize title and author
        # Valid token separators are space and underscore
        # Discard empty tokens
        title_tokens = [t for t in title.strip().replace("_", " ").split() if t]
        author_tokens = [t for t in author.strip().replace("_", " ").split() if t]

        # Clean title tokens
        title_clean = []
        for token in title_tokens:
            # Clean token is lowercase and alphanumeric
            # TODO: After Py3.7 upgrade, use .isascii()
            clean_token = "".join([char for char in token.lower() if char.isalnum()])
            # and char.isascii()])
            if clean_token and clean_token not in self.delete_words:
                title_clean.append(clean_token)

        # Clean author tokens, merge into one word
        author_word = ""
        for token in author_tokens:
            clean_token = "".join([char for char in token.lower() if char.isalnum()])
            # and char.isascii()])
            author_word += clean_token

        # Remove author_word from title, if exists (e.g. from previous make_source_id())
        while author_word in title_clean and not sanitize_only:
            title_clean.remove(author_word)

        # Select words from title for source_name
        # Use up to the first two words + last word
        if len(title_clean) >= 1:
            word1 = title_clean[0]
        else:
            # Must have at least one word
            raise ValueError("Title '{}' invalid: Must have at least one word that is not "
                             "the author name (the following words do not count: '{}')"
                             .format(title, self.delete_words))
        if len(title_clean) >= 2:
            word2 = title_clean[1]
        else:
            word2 = ""
        if len(title_clean) >= 3:
            word3 = title_clean[-1]
        else:
            word3 = ""

        # Assemble source_name
        # Strip trailing underscores from missing words
        if sanitize_only:
            source_name = "_".join(title_clean).strip("_")
        else:
            source_name = "{}_{}_{}_{}".format(author_word, word1, word2, word3).strip("_")

        # Add test flag if necessary
        if is_test:
            source_name = "_test_" + source_name

        # Determine version number to add
        # Get last Search version
        globus_secrets = get_secret()
        search_client = mdf_toolbox.confidential_login(services=['search'],
                                                       client_id=globus_secrets[
                                                           'API_CLIENT_ID'],
                                                       client_secret=globus_secrets[
                                                           'API_CLIENT_SECRET'])['search']

        # Parent record with DOI -
        #   Version records with their own DOIs
        #   Examples of this are ArXiV and Zenodo
        # Looking up the parent record DOI would take you to a landing page which shows latest metadata,
        # but list all available versions.
        # Also be able to visit the DOI of the version
        # parent: doi 10.1057/xyz   UUID1
        # version: 10.1057/xyz#v1   UUID2 (links between made with metadata) parent: UUID1
        # Only original submitter can submit a change

        old_q = {
            "q": "mdf.source_name:{} AND mdf.resource_type:dataset".format(source_name),
            "advanced": True,
            "limit": 2,  # Should only ever be one, if two are returned there's a problem
            "offset": 0
        }
        old_search = mdf_toolbox.gmeta_pop(search_client.post_search(
            mdf_toolbox.translate_index(index), old_q))

        print("Search for the old ", old_search)
        if len(old_search) == 0:
            search_version = 1
        elif len(old_search) == 1:
            search_version = old_search[0]["mdf"]["version"] + 1
        else:
            logger.error("{}: {} dataset entries found in Search: {}"
                         .format(source_name, len(old_search), old_search))
            raise ValueError("Dataset entry in Search has error")

        # Get old submission information
        scan_res = self.dyanamo_manager.scan_table(table_name="status", fields=["source_id", "user_id"],
                              filters=[("source_id", "^", source_name)])
        if not scan_res["success"]:
            logger.error("Unable to scan status database for '{}': '{}'"
                         .format(source_name, scan_res["error"]))
            raise ValueError("Dataset status has error")

        print("Scan res", scan_res)
        user_ids = set([sub["user_id"] for sub in scan_res["results"]])
        # Get most recent previous source_id and info
        old_search_version = 0
        old_sub_version = 0
        for old_sid in scan_res["results"]:
            old_sid_info = self.split_source_id(old_sid["source_id"])
            # If found more recent Search version, save both Search and sub versions
            # (sub version resets on new Search version)
            if old_sid_info["search_version"] > old_search_version:
                old_search_version = old_sid_info["search_version"]
                old_sub_version = old_sid_info["submission_version"]
            # If found more recent sub version, just save sub version
            # Search version must be the same, though
            elif (old_sid_info["search_version"] == old_search_version
                  and old_sid_info["submission_version"] > old_sub_version):
                old_sub_version = old_sid_info["submission_version"]

        # If new Search version > old Search version, sub version should reset
        if search_version > old_search_version:
            sub_version = 1
        # If they're the same, sub version should increment
        elif search_version == old_search_version:
            sub_version = old_sub_version + 1
        # Old > new is an error
        else:
            logger.error("Old Search version '{}' > new '{}': {}"
                         .format(old_search_version, search_version, source_name))
            raise ValueError("Dataset entry in Search has error")

        source_id = "{}_v{}.{}".format(source_name, search_version, sub_version)

        return {
            "source_id": source_id,
            "source_name": source_name,
            "search_version": search_version,
            "submission_version": sub_version,
            "user_id_list": user_ids
        }




    def fetch_org_rules(org_names, user_rules=None):
        """Fetch organization rules and metadata.

        Arguments:
            org_names (str or list of str): Org name or alias to fetch rules for.
            user_rules (dict): User-supplied rules to add, if desired. Default None.

        Returns:
            tuple: (list: All org canonical_names, dict: All appropriate rules)
        """

        # Normalize name: Remove special characters (including whitespace) and capitalization
        # Function for convenience, but not generalizable/useful for other cases
        def normalize_name(name):
            return "".join([c for c in name.lower() if c.isalnum()])

        # Fetch list of organizations
        schema_path = "./schemas/connect_aux_data"
        with open(os.path.join(schema_path, "organizations.json")) as organization_file:
            organizations = json.load(organization_file)

        # Cache list of all organization aliases to match against
        # Turn into tuple (normalized_aliases, org_rules) for convenience
        all_clean_orgs = []
        for org in organizations:
            aliases = [normalize_name(alias) for alias in (org.get("aliases", [])
                                                           + [org["canonical_name"]])]
            all_clean_orgs.append((aliases, org))

        if isinstance(org_names, list):
            orgs_to_fetch = org_names
        else:
            orgs_to_fetch = [org_names]
        rules = {}
        all_names = []
        # Fetch org rules and parent rules
        while len(orgs_to_fetch) > 0:
            # Process sub 0 always, so orgs processed in order
            # New org matches on canonical_name or any alias
            fetch_org = orgs_to_fetch.pop(0)
            new_org_data = [org for aliases, org in all_clean_orgs
                            if normalize_name(fetch_org) in aliases]
            if len(new_org_data) < 1:
                raise ValueError("Organization '{}' not registered in MDF Connect (from '{}')"
                                 .format(fetch_org, org_names))
            elif len(new_org_data) > 1:
                raise ValueError("Multiple organizations found with name '{}' (from '{}')"
                                 .format(fetch_org, org_names))
            new_org_data = deepcopy(new_org_data[0])

            # Check that org rules not already fetched
            if new_org_data["canonical_name"] in all_names:
                continue
            else:
                all_names.append(new_org_data["canonical_name"])

            # Add all (unprocessed) parents to fetch list
            orgs_to_fetch.extend(
                [parent for parent in new_org_data.get("parent_organizations", [])
                 if parent not in all_names])

            # Merge new rules with old
            # Strip out unneeded info
            new_org_data.pop("canonical_name", None)
            new_org_data.pop("aliases", None)
            new_org_data.pop("description", None)
            new_org_data.pop("homepage", None)
            new_org_data.pop("parent_organizations", None)
            # Save correct curation state
            if rules.get("curation", False) or new_org_data.get("curation", False):
                curation = True
            else:
                curation = False
            # Merge new rules into old rules
            rules = mdf_toolbox.dict_merge(rules, new_org_data, append_lists=True)
            # Ensure curation set if needed
            if curation:
                rules["curation"] = curation

        # Merge in user-set rules (with lower priority than any org-set rules)
        if user_rules:
            rules = mdf_toolbox.dict_merge(rules, user_rules)
            # If user set curation, set curation
            # Otherwise user preference is overridden by org preference
            if user_rules.get("curation", False):
                rules["curation"] = True

        return (all_names, rules)
