import json
import os
import sys

from mdf_connect_client import MDFConnectClient


# Data sources to use for testing - should be known-good files
DATA_SOURCES = [
    # Sample VASP outputs
    ("https://app.globus.org/file-manager?origin_id=e38ee745-6d04-11e5-ba46-22000b92c6ec"
     "&origin_path=%2Fconnect_demo%2F")
]
# Directory of test files
TEST_SUBMISSIONS_DIR = os.path.join(os.path.dirname(__file__), "deploy_suite_files")
# Time (in seconds) in between polling Connect for status
SLEEP_TIME = 10
# Codes that indicate success
SUCCESS_CODES = [
    'S',
    'M',
    'L'
]
# Codes that indicate failure
FAILURE_CODES = [
    'F',
    'R',
    'H',
    'X'  # Technically "cancelled", but cancelled tests should fail
]


def submit_test_submissions(service_instance, submission_dir=TEST_SUBMISSIONS_DIR, verbose=True):
    """Submit the test submissions to Connect.

    Arguments:
        service_instance (str): The instance of Connect to test (dev or prod).
        submission_dir (str): Path to a dir containing the files with submissions.
                Default TEST_SUBMISSIONS_DIR.
        verbose (bool): Should intermediate status messages be printed? Default True.

    Returns:
        dict: The testing results.
            success (bool): True iff all the tests succeeded.
            passed (list of str): The source_ids that passed testing.
            failed (list of dicts): The testing failures, with details.
    """
    mdfcc = MDFConnectClient(service_instance=service_instance)
    source_ids = []
    successes = []
    failures = []
    for file_name in os.listdir(submission_dir):
        path = os.path.join(submission_dir, file_name)
        if verbose:
            print("Submitting", file_name)
        with open(path) as f:
            submission = json.load(f)
        submit_res = mdfcc.submit_dataset(submission=submission)
        if not submit_res["success"]:
            if verbose:
                print("Error:", submit_res["error"])
            failures.append(submit_res)
            continue
        source_ids.append(submit_res["source_id"])
        if verbose:
            print(submit_res["source_id"], "submitted")
    if verbose:
        print("All tests submitted.")
    # Check statuses until finished
    while len(source_ids) > 0:
        sid = source_ids.pop(0)

        # If submission requires curation, accept it
        # Curation tests must have "curation" in the source_id
        if "curation" in sid and mdfcc.get_curation_task(sid, raw=True)["success"]:
            curation_res = mdfcc.accept_curation_submission(sid, reason="Testing curation",
                                                            prompt=False, raw=True)
            if not curation_res["success"]:
                if verbose:
                    print("Could not accept curation submission {}: {}"
                          .format(sid, curation_res["error"]))
                failures.append(curation_res)
                # Skip status check - test has failed
                continue

        # Now check the current status
        status = mdfcc.check_status(sid, raw=True)
        if not status["success"]:
            if verbose:
                print("Could not fetch status for", sid, status["error"])
            # Re-queue source_id
            source_ids.append(sid)
            continue
        status_res = validate_status(status["status"])
        # If failed or succeeded, put result in appropriate list
        if status_res["result"] == "failed":
            failures.append(status_res)
            if verbose:
                print(sid, "failed")
        elif status_res["result"] == "success":
            successes.append(sid)
            if verbose:
                print(sid, "passed")
        # Otherwise, is not finished and should be re-checked
        else:
            source_ids.append(sid)
    return {
        "success": (len(failures) == 0),
        "passed": successes,
        "failed": failures
    }


def validate_status(status):
    """Check a Connect status for completion.

    Arguments:
        status (dict): The MDFCS status to check.

    Returns:
        dict: The results.
    """
    # Submission is still processing
    if status["active"]:
        return {
            "result": "active"
        }
    # Submission is not processing, check for failure
    # Failure is defines as:
    #   Any failure code present in full status code
    #   Final step is not success
    if (any([code in status["status_code"] for code in FAILURE_CODES])
            or status["status_code"][-1] not in SUCCESS_CODES):
        return {
            "result": "failed",
            "status_message": status["status_message"]
        }
    # Otherwise, consider it a success
    else:
        return {
            "result": "success"
        }


########################################
# Functions to generate the test files
########################################
def generate_base_submission():
    file_name = "base_submission.json"
    path = os.path.join(TEST_SUBMISSIONS_DIR, file_name)
    mdfcc = MDFConnectClient()  # service_instance is irrelevant
    mdfcc.create_dc_block(title="Base Deploy Testing Dataset", authors="jgaff",
                          affiliations="UChicago")
    mdfcc.add_data_source(DATA_SOURCES)
    mdfcc.set_test(True)
    mdfcc.update = True
    submission = mdfcc.get_submission()
    with open(path, 'w') as f:
        json.dump(submission, f)
    return {
        "success": True
    }


def generate_integration_submission():
    file_name = "integration_submission.json"
    path = os.path.join(TEST_SUBMISSIONS_DIR, file_name)
    mdfcc = MDFConnectClient()  # service_instance is irrelevant
    mdfcc.create_dc_block(title="Integration Deploy Testing Dataset", authors="jgaff",
                          affiliations="UChicago")
    mdfcc.add_data_source(DATA_SOURCES)
    mdfcc.add_service("mdf_publish")
    # mdfcc.add_service("citrine", {"public": False})
    mdfcc.add_service("mrr")
    mdfcc.set_test(True)
    mdfcc.update = True
    submission = mdfcc.get_submission()
    with open(path, 'w') as f:
        json.dump(submission, f)
    return {
        "success": True
    }


def generate_curation_submission():
    file_name = "curation_submission.json"
    path = os.path.join(TEST_SUBMISSIONS_DIR, file_name)
    mdfcc = MDFConnectClient()  # service_instance is irrelevant
    mdfcc.create_dc_block(title="Curation Testing Dataset", authors="jgaff",
                          affiliations="UChicago")
    mdfcc.add_data_source(DATA_SOURCES)
    mdfcc.set_curation(True)
    mdfcc.set_test(True)
    mdfcc.update = True
    submission = mdfcc.get_submission()
    with open(path, 'w') as f:
        json.dump(submission, f)
    return {
        "success": True
    }


def generate_all_submissions():
    print("Base submission:", generate_base_submission()["success"])
    print("Integration submission:", generate_integration_submission()["success"])
    print("Curation_submission:", generate_curation_submission()["success"])


if __name__ == "__main__":
    # If any more options/args are added, should either use Click or remove cli support
    # This is already pushing it
    if len(sys.argv) < 2:
        print("MDF Connect Deploy Tests")
        print("========================")
        print("Arguments:")
        print("\tservice_instance: The instance of Connect to test (dev or prod).",
              "This is a required argument.")
        print("Options:")
        print("\t--no-verbose: Turn off status messages (verbose mode).",
              "Verbose mode is on by default.")
        print("\nTo regenerate all of the submissions, call this script with 'regenerate'")
        print("or 'regen' as the first argument instead of a service_instance.")
        print("No other arguments or options are permitted.")
    elif sys.argv[1] in ["regenerate", "regen"]:
        generate_all_submissions()
    elif len(sys.argv) > 3 or (len(sys.argv) == 3 and "--no-verbose" not in sys.argv):
        print("Too many arguments specified. For help, run this script with no arguments.")
    else:
        try:
            sys.argv.remove("--no-verbose")
        except ValueError:
            verbose = True
        else:
            verbose = False
        test_res = submit_test_submissions(sys.argv[1], verbose=verbose)
        if verbose:
            print("\nTesting Results:")
            print("================")
        if test_res["success"]:
            print("All tests passed.")
        else:
            print("The following tests failed:")
            [print(failure) for failure in test_res["failed"]]
        if verbose:
            print("The following tests passed:")
            print(test_res["passed"])
