resource "aws_iam_role" "admin_role" {
  name = "MDFConnectAdminRole"

  # Terraform's "jsonencode" function converts a
  # Terraform expression result to valid JSON syntax.
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole",
        Effect = "Allow",
        Principal = {
          AWS = "arn:aws:iam::557062710055:root" # Modify the service if needed
        }
      },
    ]
  })
}

# Attach the Full Access policy to the admin role
resource "aws_iam_policy_attachment" "full_access_role_attachment" {
  name = "full-access-role-attachment"
  policy_arn = aws_iam_policy.admin_access_policy.arn
  roles      = [aws_iam_role.admin_role.name]
}


# Create an inline IAM policy allowing role full access
resource "aws_iam_policy" "admin_access_policy" {
  name        = "MDFConnect-AdminAccessPolicy"
  description = "Admin policy allowing full access"
  
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = "*"
        Resource = "*"
      }
    ]
  })
}

# Create an IAM group
resource "aws_iam_group" "admin_group" {
  name = "MDFConnect-AdminGroup"
}

# Attach the IAM policy to the IAM group
resource "aws_iam_policy_attachment" "admin_group_attachment" {
  name = "admin-attachment"
  policy_arn = aws_iam_policy.admin_policy.arn
  groups      = [aws_iam_group.admin_group.name]
}

# Create an inline IAM policy
resource "aws_iam_policy" "admin_policy" {
  name        = "MDFConnect-AdminPolicy"
  description = "Admin policy allowing assume role"
  
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole",
        Effect = "Allow",
        Resource = aws_iam_role.admin_role.arn
      }
    ]
  })
}

resource "aws_iam_policy" "dynamodb_policy" {
  name        = "dynamodb-policy"
  description = "DynamoDB GetItem and PutItem Access Policy"

  # Define the policy document that grants access to DynamoDB
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action   = [
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:DeleteItem",
        ],
        Effect   = "Allow",
        Resource = "arn:aws:dynamodb:us-east-1:557062710055:table/accelerate-terraform-state-storage-locks",
      },
    ],
  })
}

resource "aws_iam_group_policy_attachment" "attach_dynamodb_policy" {
  group      = aws_iam_group.admin_group.name
  policy_arn = aws_iam_policy.dynamodb_policy.arn
}

# Create an IAM policy that grants access to the S3 bucket
resource "aws_iam_policy" "s3_access_policy" {
  name        = "S3AccessPolicy"
  description = "Policy to grant access to the S3 bucket"

  # Define the permissions for the policy
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action   = ["s3:GetObject", "s3:ListBucket"],
        Effect   = "Allow",
        Resource = [
          "arn:aws:s3:::accelerate-terraform-state-storage/*", # Replace with your S3 bucket name
          "arn:aws:s3:::accelerate-terraform-state-storage",    # Replace with your S3 bucket name
        ],
      },
      {
        Action   = ["s3:PutObject", "s3:DeleteObject"],
        Effect   = "Allow",
        Resource = "arn:aws:s3:::accelerate-terraform-state-storage/*", # Replace with your S3 bucket name
      },
    ],
  })
}

# Attach the S3 access policy to the IAM group
resource "aws_iam_group_policy_attachment" "s3_access_attachment" {
  group      = aws_iam_group.admin_group.name
  policy_arn = aws_iam_policy.s3_access_policy.arn
}

# Add the IAM group to the role's trusted entities
resource "aws_iam_role_policy_attachment" "admin_role_attachment" {
  policy_arn = aws_iam_policy.admin_policy.arn
  role       = aws_iam_role.admin_role.name
}

# Output the role ARN for reference
output "role_arn" {
  value = aws_iam_role.admin_role.arn
}

terraform {
  backend "s3" {
    # Replace this with your bucket name!
    bucket         = "accelerate-terraform-state-storage"
    key            = "terraform/AccelerateRoles/terraform.tfstate"
    region         = "us-east-1"

    # Replace this with your DynamoDB table name!
    dynamodb_table = "accelerate-terraform-state-storage-locks"
    encrypt        = true
  }
}
