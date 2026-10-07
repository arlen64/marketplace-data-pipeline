import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    max,
    min,
    round as spark_round,
)

# Configura o sistema de logs para registrar informações, avisos e erros durante a execução
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# Cria o logger utilizado pelo script
logger = logging.getLogger(__name__)


# Define os caminhos das camadas Silver e Gold
BASE_DIR = Path(__file__).resolve().parent.parent

silver_path = BASE_DIR / "data_lake" / "silver" / "products.parquet"
gold_dir = BASE_DIR / "data_lake" / "gold"

# Garante que o diretório Gold exista
gold_dir.mkdir(parents=True, exist_ok=True)

gold_file = gold_dir / "category_metrics.parquet"


# Valida se a camada Silver existe antes do processamento
if not silver_path.exists():
    raise FileNotFoundError(
        f"Camada Silver não encontrada: {silver_path}"
    )


# Cria a sessão Spark
spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("SilverToGold")
    .getOrCreate()
)


try:
    # Registra o início da transformação
    logger.info("Iniciando transformação Silver -> Gold.")

    # Lê os dados tratados da camada Silver em formato Parquet
    df = spark.read.parquet(str(silver_path))

    # Valida se existem registros para agregar
    if df.rdd.isEmpty():
        raise ValueError("A camada Silver está vazia.")

    # Valida se as colunas necessárias existem
    required_columns = {
        "product_id",
        "price",
        "category",
    }

    missing_columns = required_columns - set(df.columns)

    if missing_columns:
        raise ValueError(
            f"Colunas obrigatórias ausentes na Silver: {missing_columns}"
        )

    # Agrupa os produtos por categoria e calcula métricas
    df_gold = (
        df.groupBy("category")
        .agg(
            count("product_id").alias("total_products"),
            spark_round(avg("price"), 2).alias("avg_price"),
            spark_round(min("price"), 2).alias("min_price"),
            spark_round(max("price"), 2).alias("max_price")
        )
        .orderBy(col("avg_price").desc())
    )

    # Valida se alguma métrica foi gerada
    if df_gold.rdd.isEmpty():
        raise ValueError(
            "Nenhuma métrica foi gerada para a camada Gold."
        )

    # Salva a camada Gold em formato Parquet usando o próprio Spark
    df_gold.write \
        .mode("overwrite") \
        .parquet(str(gold_file))

    logger.info(
        f"Transformação Silver -> Gold concluída com sucesso. "
        f"Categorias processadas: {df_gold.count()}. "
        f"Arquivo Gold: {gold_file}"
    )

except Exception as error:
    # Registra o erro para facilitar o diagnóstico no Airflow
    logger.error(f"Erro na transformação Silver -> Gold: {error}")
    raise

finally:
    # Encerra a sessão Spark mesmo em caso de falha
    spark.stop()