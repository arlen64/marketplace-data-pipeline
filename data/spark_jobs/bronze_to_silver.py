import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Configura o sistema de logs para registrar informações, avisos e erros durante a execução
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# Cria o logger utilizado pelo script
logger = logging.getLogger(__name__)

# Define os caminhos das camadas Bronze e Silver
BASE_DIR = Path(__file__).resolve().parent.parent

bronze_path = BASE_DIR / "data_lake" / "bronze" / "products.json"
silver_dir = BASE_DIR / "data_lake" / "silver"

# Garante que o diretório Silver exista
silver_dir.mkdir(parents=True, exist_ok=True)

silver_file = silver_dir / "products.parquet"

# Valida se o arquivo Bronze existe antes de iniciar o processamento
if not bronze_path.exists():
    raise FileNotFoundError(
        f"Arquivo Bronze não encontrado: {bronze_path}"
    )


# Cria a sessão Spark
spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("BronzeToSilver")
    .getOrCreate()
)

try:
    # Registra o Carregamento dos dados para camada silver
    logger.info("Iniciando transformação Bronze -> Silver.")

    # Lê o JSON da Bronze, permitindo registros distribuídos em várias linhas
    df = (
        spark.read
        .option("multiLine", "true")
        .json(str(bronze_path))
    )

    # Valida se existem registros para processar
    if df.rdd.isEmpty():
        raise ValueError("A camada Bronze está vazia.")

    # Seleciona e padroniza os campos que farão parte da camada Silver
    df_silver = df.select(
        col("id").alias("product_id"),
        col("title"),
        col("price"),
        col("category")
    )
    # Valida se a transformação gerou registros
    if df_silver.rdd.isEmpty():
        raise ValueError(
            "Nenhum registro foi gerado para a camada Silver."
    )

# Salva a camada Silver em formato Parquet usando o próprio Spark
    df_silver.write \
        .mode("overwrite") \
        .parquet(str(silver_file))

  
    logger.info(
        f"Transformação Bronze -> Silver concluída com sucesso. "
        f"Registros processados: {df_silver.count()}. "
        f"Arquivo Silver: {silver_file}"
    )

except Exception as error:
    # Registra o erro para facilitar o diagnóstico nos logs do Airflow
    logger.error(f"Erro na transformação Bronze -> Silver: {error}")
    raise

finally:
    # Encerra a sessão Spark mesmo que ocorra algum erro
    spark.stop()