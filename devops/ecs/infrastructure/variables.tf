# data "aws_caller_identity" "current" {}

locals {
  env        = terraform.workspace == "default" ? "dev" : terraform.workspace
  project    = "defi-features-data-pipeline"
  team       = "data-engineering"
  account_id = "account_id" #TODO: Remove this hardcode and use data.aws_caller_identity.current.account_id
}

variable "ecr_repo_url" {
  type        = string
  description = "URI of the ECR repository"
  default     = "account_id.dkr.ecr.us-east-2.amazonaws.com/data-lakehouse/defi-features-data-pipeline"
}

variable "ecr_repo_image_tag" {
  type        = string
  description = "Tag of the ECR repository"
  default     = "latest"
}

variable "aws_region" {
  type        = string
  description = "AWS region"
  default     = "us-east-2"
}
