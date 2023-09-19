provider "aws" {
#  shared_config_files      = ["/Users/blau/.aws/config"]
#  shared_credentials_files = ["/Users/blau/.aws/credentials"]
#  profile                  = "Accelerate"
   assume_role {
      role_arn = "arn:aws:iam::557062710055:role/MDFConnectAdminRole"
   }
}
