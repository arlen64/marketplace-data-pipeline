
resource "aws_s3_bucket" "marketplace" {
  bucket = "marketplace-data-arlen64-2026"

  tags = {
    Environment = "Development"
    Project     = "MarketplaceDataPipeline"
  }
  # Proteção contra planos de destruição ou substituição
  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_s3_bucket_public_access_block" "marketplace" {
  bucket = aws_s3_bucket.marketplace.id

  block_public_acls       = true
  ignore_public_acls      = true
  block_public_policy     = true
  restrict_public_buckets = true
}
# Declarar criptografia existente
resource "aws_s3_bucket_server_side_encryption_configuration" "marketplace" {
  bucket = aws_s3_bucket.marketplace.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}
# Declarar versionamento no terraform
resource "aws_s3_bucket_versioning" "marketplace" {
  bucket = aws_s3_bucket.marketplace.id

  versioning_configuration {
    status = "Enabled"
  }
}
# Criar uma politica de retenção
resource "aws_s3_bucket_lifecycle_configuration" "marketplace" {
  bucket = aws_s3_bucket.marketplace.id

  rule {
    id     = "cleanup-old-versions"
    status = "Enabled"

    filter {
      prefix = ""
    }
    # Manter versões anteriores por 7 dias
    noncurrent_version_expiration {
      noncurrent_days = 7
    }
  }

  depends_on = [
    aws_s3_bucket_versioning.marketplace
  ]
}

