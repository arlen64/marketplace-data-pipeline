import logging
from pathlib import Path
import boto3
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    max,
    min,
    round as spark_round,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent

silver_path = BASE_DIR / "data_lake" / "silver" / "products.parquet"
gold_dir = BASE_DIR / "data_lake" / "gold"
gold_dir.mkdir(parents=True, exist_ok=True)
gold_file = gold_dir / "category_metrics.parquet"

s3_client = boto3.client("s3")
bucket_name = "marketplace-data-arlen64-2026"
s3_silver_prefix = "silver/products.parquet/"

logger.info("Baixando camada Silver do S3...")

response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_silver_prefix)
for obj in response.get("Contents", []):
    key = obj["Key"]
    if key.endswith("/"):
        continue
    local_file = silver_path / Path(key).name
    local_file.parent.mkdir(parents=True, exist_ok=True)
    s3_client.download_file(bucket_name, key, str(local_file))

logger.info("Download da Silver concluído.")

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("SilverToGold")
    .getOrCreate()
)

try:
    logger.info("Iniciando transformação Silver -> Gold.")

    df = spark.read.parquet(str(silver_path))

    if df.rdd.isEmpty():
        raise ValueError("A camada Silver está vazia.")

    required_columns = {"product_id", "price", "category"}
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise ValueError(f"Colunas obrigatórias ausentes: {missing_columns}")

    df_gold = (
        df.groupBy("category")
        .agg(
            count("product_id").alias("total_products"),
            spark_round(avg("price"), 2).alias("avg_price"),
            spark_round(min("price"), 2).alias("min_price"),
            spark_round(max("price"), 2).alias("max_price"),
        )
        .orderBy(col("avg_price").desc())
    )

    if df_gold.rdd.isEmpty():
        raise ValueError("Nenhuma métrica foi gerada para a Gold.")

    # Validações na Gold
    if df_gold.filter(col("total_products") <= 0).count() > 0:
        raise ValueError("Existem categorias com total_products <= 0.")

    if df_gold.filter(col("avg_price") <= 0).count() > 0:
        raise ValueError("Existem categorias com avg_price <= 0.")

    logger.info(f"Qualidade da Gold OK. {df_gold.count()} categorias.")

    # Limpa destino local
    if gold_file.exists():
        shutil.rmtree(gold_file)
        logger.info(f"Pasta Gold local limpa: {gold_file}")

    df_gold.write.mode("overwrite").parquet(str(gold_file))

    logger.info(
        f"Silver -> Gold concluída. "
        f"Categorias: {df_gold.count()}. "
        f"Destino: {gold_file}"
    )

    # Limpa versões antigas no S3
    logger.info("Limpando arquivos antigos da Gold no S3...")
    response = s3_client.list_objects_v2(
        Bucket=bucket_name, Prefix="gold/category_metrics.parquet/"
    )
    if "Contents" in response:
        objects_to_delete = [{"Key": obj["Key"]} for obj in response["Contents"]]
        s3_client.delete_objects(
            Bucket=bucket_name, Delete={"Objects": objects_to_delete}
        )
        logger.info(f"Removidos {len(objects_to_delete)} arquivos antigos.")

    # Upload da nova versão
    logger.info("Enviando Gold para o S3...")
    for file_path in gold_file.rglob("*"):
        if file_path.is_file():
            relative_path = file_path.relative_to(gold_file.parent)
            s3_key = f"gold/{relative_path.as_posix()}"
            s3_client.upload_file(str(file_path), bucket_name, s3_key)

    logger.info("Upload da Gold concluído.")

except Exception as error:
    logger.error(f"Erro na transformação Silver -> Gold: {error}")
    raise

finally:
    spark.stop()