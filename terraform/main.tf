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
