resource "aws_ecr_repository" "mdf-connect-lambda-repo" {
  for_each             = var.functions
  name                 = "mdf-lambdas/${each.key}"
  image_tag_mutability = "MUTABLE"
  image_scanning_configuration {
    scan_on_push = true
  }
}
