variable "env" {
  description = "The environment for the deployment. (dev, prod)"
  type        = string
}

variable "namespace" {
  description = "The namespace for the deployment."
  type        = string
}

variable "env_vars" {
  description = "Set of environment variables for the functions."
  type = map(string)
}