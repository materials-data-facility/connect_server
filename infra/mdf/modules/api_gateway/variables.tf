variable "env" {
  type = string
  description = "Either 'dev' or 'prod'"
}

variable "mdf_connect_authorizer_invoke_arn" {
  description = "The invoke ARN of the GardenAuthorizer Lambda function"
  type        = string
}


variable "mdf_connect_authorizer_function_name" {
  description = "The function name of the GardenAuthorizer Lambda function"
  type        = string
}

variable "submit_lambda_invoke_arn" {
  description = "The invoke ARN of the Submit Lambda function"
  type        = string
}

variable "submit_lambda_function_name" {
  description = "The name of the Submit Lambda function"
  type        = string
}

variable "status_lambda_invoke_arn" {
  description = "The invoke ARN of the Status Lambda function"
  type        = string
}

variable "status_lambda_function_name" {
  description = "The name of the Status Lambda function"
  type        = string
}

variable "submissions_lambda_invoke_arn" {
    description = "The invoke ARN of the Submissions Lambda function"
    type        = string
}

variable "submissions_lambda_function_name" {
    description = "The name of the Submissions Lambda function"
    type        = string
}



