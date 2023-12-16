variable "namespace" {
  type = string
  description = "Namespace to deploy to"
  default = "MDF-Connect2"
}

variable "GitHubOrg" {
    type = string
    description = "GitHub organization to pull code from"
    default = "materials-data-facility"
}

variable "GitHubRepo" {
    type = string
    description = "GitHub repository to pull code from"
    default = "connect_server"
}

variable "functions" {
  type = set(string)
  description = "List of functions that need to have ECR repos created for them"
    default = [
        "auth",
        "submit",
        "status",
        "submissions",
    ]
}