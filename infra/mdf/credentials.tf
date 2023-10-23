provider "aws" {
   assume_role {
      role_arn = "arn:aws:iam::557062710055:role/MDFConnectAdminRole"
   }
}
