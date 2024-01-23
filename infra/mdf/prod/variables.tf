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
        RUN_AS_SCOPE="4c37a999-da4b-4969-b621-58bfb243c5bc"
        SEARCH_INDEX_UUID="1a57bbe5-5272-477f-9d31-343b8258b7a5"
        TEST_DATA_DESTINATION="globus://f10a69a9-338c-4e5b-baa1-0dc92359ab47/mdf_testing/"
        TEST_SEARCH_INDEX_UUID="5acded0c-a534-45af-84be-dcf042e36412"
        FLOW_ID="4c37a999-da4b-4969-b621-58bfb243c5bc"
        FLOW_SCOPE= "https://auth.globus.org/scopes/4c37a999-da4b-4969-b621-58bfb243c5bc/flow_4c37a999_da4b_4969_b621_58bfb243c5bc_user"
        REQUIRED_GROUP_MEMBERSHIP="cc192dca-3751-11e8-90c1-0a7c735d220a"
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

variable "resource_tags" {
    type = map(string)
    default = {
        "Owner" = "MDF"
        "Environment" = "Production"
        "Project" = "MDF Connect"
    }
}