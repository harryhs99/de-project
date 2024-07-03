terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "5.8.0"
    }
  }
# NOTE to team: This is my secrets bucket which terraform has no control over and is used to store the terraform state file
  backend "s3" {
    bucket = "bucket-of-keys20230714102552775100000001"
    key    = "project/terraform.tfstate"
    region = "eu-west-2"
  }
}

provider "aws" {
  region = "eu-west-2"
}

data "aws_caller_identity" "current" {}

data "aws_region" "current" {
  name = "eu-west-2"
}