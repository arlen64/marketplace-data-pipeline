import logging
import os
from pathlib import Path

import boto3
from pyspark.sql import SparkSession
from sqlalchemy import create_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
gold_path = BASE_DIR / "data_lake" / "gold" / "category_metrics.parquet"

# Configurações do banco analítico
DB_USER = os.getenv("MARKETPLACE_DB_USER")
DB_PASSWORD = os.getenv("MARKETPLACE_DB_PASSWORD")
DB_HOST = os.getenv("MARKETPLACE_DB_HOST")
DB_PORT = os.getenv("MARKETPLACE_DB_PORT")
DB_NAME = os.getenv("MARKETPLACE_DB_NAME")

required_env = {
    "MARKETPLACE_DB_USER": DB_USER,
    "MARKETPLACE_DB_PASSWORD": DB_PASSWORD,
    "MARKETPLACE_DB_HOST": DB_HOST,
    "MARKETPLACE_DB_PORT": DB_PORT,
    "MARKETPLACE_DB_NAME": DB_NAME,
}

missing = [k for k, v in required_env.items() if not v]
if missing:
    raise ValueError(f"Variáveis de ambiente ausentes: {missing}")

# Baixa a Gold mais recente do S3
s3_client = boto3.client("s3")
bucket_name = "marketplace-data-arlen64-2026"
s3_gold_prefix = "gold/category_metrics.parquet/"

logger.info("Baixando camada Gold do S3...")
response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_gold_prefix)

for obj in response.get("Contents", []):
    key = obj["Key"]
    if key.endswith("/"):
        continue
    local_file = gold_path / Path(key).name
    local_file.parent.mkdir(parents=True, exist_ok=True)
    s3_client.download_file(bucket_name, key, str(local_file))

logger.info("Download da Gold concluído.")

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("LoadGoldToPostgres")
    .getOrCreate()
)

try:
    logger.info("Iniciando carga Gold -> PostgreSQL.")

    df_gold = spark.read.parquet(str(gold_path))

    if df_gold.rdd.isEmpty():
        raise ValueError("A camada Gold está vazia.")

    # Validações antes da carga
    expected_columns = {
        "category", "total_products", "avg_price", "min_price", "max_price"
    }
    missing_cols = expected_columns - set(df_gold.columns)
    if missing_cols:
        raise ValueError(f"Colunas ausentes na Gold: {missing_cols}")

    if df_gold.filter("total_products <= 0 OR avg_price <= 0").count() > 0:
        raise ValueError("Gold contém métricas inválidas (valores <= 0).")

    row_count = df_gold.count()
    logger.info(f"Gold validada. {row_count} categorias prontas para carga.")

    pdf_gold = df_gold.toPandas()

    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    pdf_gold.to_sql(
        "gold_category_metrics",
        engine,
        if_exists="replace",
        index=False,
    )

    logger.info(f"Carga concluída. {len(pdf_gold)} linhas inseridas no PostgreSQL.")

except Exception as error:
    logger.error(f"Erro ao carregar Gold no PostgreSQL: {error}")
    raise

finally:
    spark.stop()