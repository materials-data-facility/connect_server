# Create a namespace
variable locals {
  namespace = "MDF-Connect"
  GitHubOrg = "materials-data-facility"
  GitHubRepo = "connect_server"
  envs = ["test", "prod"]
  environments = toset(local.envs)
  funcs = ["auth", "submit", "status"]
  functions = toset(local.funcs)
  account_id = data.aws_caller_identity.current.account_id
  region         = "us-east-1"

}
