import logging
import os
from pathlib import Path

from pyspark.sql import SparkSession
from sqlalchemy import create_engine


# Configura o sistema de logs para registrar informações, avisos e erros durante a execução
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# Cria o logger utilizado pelo script
logger = logging.getLogger(__name__)


# Define o diretório base do projeto e o caminho da camada Gold
BASE_DIR = Path(__file__).resolve().parent.parent

gold_path = (
    BASE_DIR
    / "data_lake"
    / "gold"
    / "category_metrics.parquet"
)


# Recupera as configurações do PostgreSQL pelas variáveis de ambiente
DB_USER = os.getenv("MARKETPLACE_DB_USER")
DB_PASSWORD = os.getenv("MARKETPLACE_DB_PASSWORD")
DB_HOST = os.getenv("MARKETPLACE_DB_HOST")
DB_PORT = os.getenv("MARKETPLACE_DB_PORT")
DB_NAME = os.getenv("MARKETPLACE_DB_NAME")


# Valida se todas as configurações do banco foram carregadas
db_config = {
    "MARKETPLACE_DB_USER": DB_USER,
    "MARKETPLACE_DB_PASSWORD": DB_PASSWORD,
    "MARKETPLACE_DB_HOST": DB_HOST,
    "MARKETPLACE_DB_PORT": DB_PORT,
    "MARKETPLACE_DB_NAME": DB_NAME,
}

missing_config = [
    key
    for key, value in db_config.items()
    if not value
]

if missing_config:
    raise ValueError(
        f"Variáveis de ambiente ausentes: {missing_config}"
    )


# Valida se a camada Gold existe antes da carga
if not gold_path.exists():
    raise FileNotFoundError(
        f"Camada Gold não encontrada: {gold_path}"
    )


# Cria a sessão Spark para leitura do Parquet
spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("LoadGoldToPostgres")
    .getOrCreate()
)


try:
    # Registra o início do carregamento
    logger.info("Iniciando carregamento Gold -> PostgreSQL.")

    # Lê a camada Gold em formato Parquet
    df_gold = spark.read.parquet(str(gold_path))

    # Impede a carga caso a Gold esteja vazia
    if df_gold.rdd.isEmpty():
        raise ValueError("A camada Gold está vazia.")

    # Converte apenas o resultado agregado para Pandas
    # A Gold possui poucos registros, portanto essa conversão é segura neste projeto
    pdf_gold = df_gold.toPandas()

    # Cria a conexão com o PostgreSQL
    engine = create_engine(
        f"postgresql+psycopg2://"
        f"{DB_USER}:{DB_PASSWORD}@"
        f"{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    # Carrega os dados da camada Gold na tabela PostgreSQL
    pdf_gold.to_sql(
        "gold_category_metrics",
        engine,
        if_exists="replace",
        index=False
    )

    logger.info(
        f"Gold carregada com sucesso no PostgreSQL. "
        f"Linhas carregadas: {len(pdf_gold)}"
    )

except Exception as error:
    # Registra o erro para facilitar o diagnóstico no Airflow
    logger.error(
        f"Erro ao carregar Gold no PostgreSQL: {error}"
    )
    raise

finally:
    # Encerra a sessão Spark mesmo em caso de falha
    spark.stop()