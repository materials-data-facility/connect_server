variable "env" {
  type = string
  description = "Either 'dev' or 'prod'"
  default = "prod"
}

variable "namespace" {
  type = string
  description = "Namespace to deploy to"
  default = "MDF-Connect2"
}

variable "mdf_secrets_arn" {
  type = string
  description = "ARN of the MDF Secrets Manager"
  default = "arn:aws:secretsmanager:us-east-1:557062710055:secret:MDF-Connect-Secrets-prod-kKiDe8"
}


#These are the env vars provided to the testlambda functions
#Edit them here for your deployment
variable "env_vars" {
  type = map(string)
  default = {
        DYNAMO_STATUS_TABLE="<<Will be updated by Terraform>>"
        MDF_SECRETS_NAME="MDF-Connect-Secrets-prod"
        MDF_AWS_REGION="us-east-1"
        GDRIVE_EP="f00dfd6c-edf4-4c8b-a4b1-be6ad92a4fbb"
        GDRIVE_ROOT="/Shared With Me"
        MANAGE_FLOWS_SCOPE="https://auth.globus.org/scopes/eec9b274-0c81-4334-bdc2-54e90e689b9a/manage_flows"
        MONITOR_BY_GROUP="urn:globus:groups:id:5fc63928-3752-11e8-9c6f-0e00fd09bf20"
        PORTAL_URL="https://acdc.alcf.anl.gov/mdf/detail/"
        RUN_AS_SCOPE="aa5f9e85-5305-4f9e-9679-95c158e5aa47"
        SEARCH_INDEX_UUID="1a57bbe5-5272-477f-9d31-343b8258b7a5"
        TEST_DATA_DESTINATION="globus://f10a69a9-338c-4e5b-baa1-0dc92359ab47/mdf_testing/"
        TEST_SEARCH_INDEX_UUID="5acded0c-a534-45af-84be-dcf042e36412"
        FLOW_ID="c096e223-59e8-42fe-85ea-956208b0f878"
        FLOW_SCOPE= "https://auth.globus.org/scopes/c096e223-59e8-42fe-85ea-956208b0f878/flow_c096e223_59e8_42fe_85ea_956208b0f878_user"
        }
}

variable "ecr_repos" {
  type = map(string)
  default = {
    "submit" = "557062710055.dkr.ecr.us-east-1.amazonaws.com/mdf-lambdas/submit"
    "submissions" = "557062710055.dkr.ecr.us-east-1.amazonaws.com/mdf-lambdas/submissions"
    "status" = "557062710055.dkr.ecr.us-east-1.amazonaws.com/mdf-lambdas/status"
    "auth" = "557062710055.dkr.ecr.us-east-1.amazonaws.com/mdf-lambdas/auth"
  }
}