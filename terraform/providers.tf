terraform {
  required_version = ">= 1.5.0" # Ultilizando uma versão mais estável

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region  = "us-east-1"
  profile = "terraform-admin"
}
