import logging
import shutil
from pathlib import Path

import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent

bronze_path = BASE_DIR / "data_lake" / "bronze" / "products.json"
silver_dir = BASE_DIR / "data_lake" / "silver"
silver_dir.mkdir(parents=True, exist_ok=True)
silver_file = silver_dir / "products.parquet"

s3_client = boto3.client("s3")
bucket_name = "marketplace-data-arlen64-2026"
s3_bronze_key = "bronze/products.json"

logger.info("Baixando arquivo Bronze do S3...")
s3_client.download_file(bucket_name, s3_bronze_key, str(bronze_path))
logger.info(f"Bronze baixado para: {bronze_path}")

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("BronzeToSilver")
    .getOrCreate()
)

try:
    logger.info("Iniciando transformação Bronze -> Silver.")

    df = (
        spark.read
        .option("multiLine", "true")
        .json(str(bronze_path))
    )

    if df.rdd.isEmpty():
        raise ValueError("A camada Bronze está vazia.")

    df_silver = df.select(
        col("id").alias("product_id"),
        col("title"),
        col("price"),
        col("category")
    )

    if df_silver.rdd.isEmpty():
        raise ValueError("Nenhum registro foi gerado para a camada Silver.")

    # Checagens rápidas de qualidade na Silver
    null_counts = df_silver.select([
        col(c).isNull().cast("int").alias(c) for c in df_silver.columns
    ]).groupBy().sum().collect()[0]

    for field in ["product_id", "title", "price", "category"]:
        nulls = null_counts[f"sum({field})"]
        if nulls and nulls > 0:
            raise ValueError(f"Campo '{field}' possui {nulls} valores nulos na Silver.")

    # Preço deve ser positivo
    invalid_prices = df_silver.filter(col("price") <= 0).count()
    if invalid_prices > 0:
        raise ValueError(f"Encontrados {invalid_prices} produtos com preço <= 0.")

    logger.info("Qualidade de dados da Silver OK.")

    # Limpa destino local antes de gravar
    if silver_file.exists():
        shutil.rmtree(silver_file)
        logger.info(f"Pasta Silver local limpa: {silver_file}")

    df_silver.write.mode("overwrite").parquet(str(silver_file))

    logger.info(
        f"Transformação Bronze -> Silver concluída. "
        f"Registros: {df_silver.count()}. "
        f"Destino: {silver_file}"
    )

    # Remove versões antigas no S3 antes do novo upload
    logger.info("Limpando arquivos antigos da Silver no S3...")
    response = s3_client.list_objects_v2(
        Bucket=bucket_name, Prefix="silver/products.parquet/"
    )
    if "Contents" in response:
        objects_to_delete = [{"Key": obj["Key"]} for obj in response["Contents"]]
        s3_client.delete_objects(
            Bucket=bucket_name, Delete={"Objects": objects_to_delete}
        )
        logger.info(f"Removidos {len(objects_to_delete)} arquivos antigos.")

    # Sobe a nova versão
    logger.info("Enviando Silver para o S3...")
    for file_path in silver_file.rglob("*"):
        if file_path.is_file():
            relative_path = file_path.relative_to(silver_file.parent)
            s3_key = f"silver/{relative_path.as_posix()}"
            s3_client.upload_file(str(file_path), bucket_name, s3_key)

    logger.info("Upload da Silver concluído.")

except Exception as error:
    logger.error(f"Erro na transformação Bronze -> Silver: {error}")
    raise

finally:
    spark.stop()
