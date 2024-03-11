terraform {
  required_version = ">= 0.13"

  backend "s3" {
    bucket = "spectral-terraform-state"
    key    = "tf-worskpaces/defi-features-data-pipeline/terraform.tfstate"
    region = "us-east-2"
  }

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 3.0, < 5.0"
    }
    docker = {
      source  = "kreuzwerker/docker"
      version = "2.15.0"
    }
  }
}
