#This is commented out as a openid_connect_provider already exists in the Accelerate account--and there can be only one.  If bootstrapping in a different account, you will need to uncomment this..
#resource "aws_iam_openid_connect_provider" "MDFgithub" {
# url = "https://token.actions.githubusercontent.com"
#
# client_id_list = [
#   "sts.amazonaws.com"
# ]
#
# thumbprint_list = ["a031c46782e6e6c662c2c87c76da9aa62ccabd8e"]
#}

data "aws_iam_policy_document" "github_allow" {
 statement {
   effect  = "Allow"
   actions = ["sts:AssumeRoleWithWebIdentity"]
   principals {
     type        = "Federated"
     #If you uncommented the MDFgithub resource above, you'll need this identifier:
     #identifiers = [aws_iam_openid_connect_provider.MDFgithub.arn]
     #This identifier is hardcoded to preexisting one in the Accelerate account:
     identifiers = ["arn:aws:iam::557062710055:oidc-provider/token.actions.githubusercontent.com"]
   }
   condition {
     test     = "StringLike"
 variable = "token.actions.githubusercontent.com:sub"
     values   = ["repo:${local.GitHubOrg}/${local.GitHubRepo}:*"]

   }
 }
}

 resource "aws_iam_role" "github_role" {
 name               = "${local.namespace}GithubActionsRole"
 assume_role_policy = data.aws_iam_policy_document.github_allow.json
}

resource "aws_iam_role_policy_attachment" "lambda-full-access-policy-attachment" {
    role = "${aws_iam_role.github_role.name}"
    policy_arn = "arn:aws:iam::aws:policy/AWSLambda_FullAccess"
}

resource "aws_iam_role_policy_attachment" "ecr-full-access-policy-attachment" {
    role = "${aws_iam_role.github_role.name}"
    policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryFullAccess"
}

