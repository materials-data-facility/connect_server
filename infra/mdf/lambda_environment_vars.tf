#These are the env vars provided to the prod lambda functions
#Edit them here for your deployment
variable "prod_env_vars" {
  type = map
  default = {
        DYNAMO_STATUS_TABLE="MDF-Connect-prod"
        MDF_SECRETS_NAME="MDF-Connect-Secrets-prod"
        MDF_AWS_REGION="us-east-1"
        GDRIVE_EP="f00dfd6c-edf4-4c8b-a4b1-be6ad92a4fbb"
        GDRIVE_ROOT="/Shared With Me"
        MANAGE_FLOWS_SCOPE="https://auth.globus.org/scopes/eec9b274-0c81-4334-bdc2-54e90e689b9a/manage_flows"
        MONITOR_BY_GROUP="urn:globus:groups:id:5fc63928-3752-11e8-9c6f-0e00fd09bf20"
        PORTAL_URL="https://acdc.alcf.anl.gov/mdf/detail/"
        RUN_AS_SCOPE="0c7ee169-cefc-4a23-81e1-dc323307c863"
        SEARCH_INDEX_UUID="1a57bbe5-5272-477f-9d31-343b8258b7a5"
        TEST_DATA_DESTINATION="globus://f10a69a9-338c-4e5b-baa1-0dc92359ab47/mdf_testing/"
        TEST_SEARCH_INDEX_UUID="5acded0c-a534-45af-84be-dcf042e36412"
        FLOW_ID="5b016e00-4518-4a94-b2f0-2724cab7ca08"
        FLOW_SCOPE= "https://auth.globus.org/scopes/5b016e00-4518-4a94-b2f0-2724cab7ca08/flow_5b016e00_4518_4a94_b2f0_2724cab7ca08_user"
        }
}
#These are the env vars provided to the testlambda functions
#Edit them here for your deployment
variable "test_env_vars" {
  type = map
  default = {
        DYNAMO_STATUS_TABLE="MDF-Connect-test"
        MDF_SECRETS_NAME="MDF-Connect-Secrets-test"
        MDF_AWS_REGION="us-east-1"
        GDRIVE_EP="f00dfd6c-edf4-4c8b-a4b1-be6ad92a4fbb"
        GDRIVE_ROOT="/Shared With Me"
        MANAGE_FLOWS_SCOPE="https://auth.globus.org/scopes/eec9b274-0c81-4334-bdc2-54e90e689b9a/manage_flows"
        MONITOR_BY_GROUP="urn:globus:groups:id:5fc63928-3752-11e8-9c6f-0e00fd09bf20"
        PORTAL_URL="https://acdc.alcf.anl.gov/mdf/detail/"
        RUN_AS_SCOPE="0c7ee169-cefc-4a23-81e1-dc323307c863"
        SEARCH_INDEX_UUID="ab71134d-0b36-473d-aa7e-7b19b2124c88"
        TEST_DATA_DESTINATION="globus://f10a69a9-338c-4e5b-baa1-0dc92359ab47/mdf_testing/"
        TEST_SEARCH_INDEX_UUID="ab71134d-0b36-473d-aa7e-7b19b2124c88"
        FLOW_ID="0c7ee169-cefc-4a23-81e1-dc323307c863"
        FLOW_SCOPE= "https://auth.globus.org/scopes/0c7ee169-cefc-4a23-81e1-dc323307c863/flow_0c7ee169_cefc_4a23_81e1_dc323307c863_user"
        }
}
