terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.80"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  # Remote backend: local runs and GitHub Actions CI share the same state.
  # The bucket + DynamoDB lock table are created out-of-band (not by this
  # Terraform) to avoid the chicken-and-egg problem.
  backend "s3" {
    bucket         = "wistia-analytics-tfstate-041282018868"
    key            = "wistia-pipeline/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "wistia-analytics-tflock"
    encrypt        = true
  }
}

provider "aws" {
  region  = var.aws_region
  profile = var.aws_profile

  default_tags {
    tags = {
      Project     = "wistia-video-analytics"
      ManagedBy   = "terraform"
      Environment = var.environment
      Owner       = "data-engineering"
    }
  }
}

data "aws_caller_identity" "current" {}

locals {
  account_id   = data.aws_caller_identity.current.account_id
  name_prefix  = "wistia-analytics"
  bucket_raw   = "${local.name_prefix}-raw-${local.account_id}"
  bucket_proc  = "${local.name_prefix}-processed-${local.account_id}"
  bucket_scripts = "${local.name_prefix}-scripts-${local.account_id}"
}
