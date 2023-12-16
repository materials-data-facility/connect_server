output "ecr_repositories" {
  value = {
    for key, value in aws_ecr_repository.mdf-connect-lambda-repo : key => value.repository_url
  }
}