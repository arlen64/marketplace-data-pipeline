
data "aws_iam_policy_document" "pipeline_s3" {
  statement {
    sid    = "ListMarketplaceBucket"
    effect = "Allow"

    actions = [
      "s3:ListBucket"
    ]

    resources = [
      aws_s3_bucket.marketplace.arn
    ]
  }

  statement {
    sid    = "ReadWriteMarketplaceObjects"
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject"
    ]

    resources = [
      "${aws_s3_bucket.marketplace.arn}/*"
    ]
  }
}
# Criar usuario com permisões expecificas
resource "aws_iam_policy" "pipeline_s3" {
  name        = "marketplace-pipeline-s3-policy"
  description = "Permissoes S3 para o pipeline de engenharia de dados"

  policy = data.aws_iam_policy_document.pipeline_s3.json

  tags = {
    Environment = "Development"
    Project     = "MarketplaceDataPipeline"
  }
}

# Usuário IAM exclusivo para o pipeline (menor privilégio)
resource "aws_iam_user" "pipeline" {
  name = "marketplace-pipeline-user"
  path = "/"

  tags = {
    Environment = "Development"
    Project     = "MarketplaceDataPipeline"
  }
}

# Anexa a política S3 ao usuário do pipeline
resource "aws_iam_user_policy_attachment" "pipeline_s3" {
  user       = aws_iam_user.pipeline.name
  policy_arn = aws_iam_policy.pipeline_s3.arn
}
