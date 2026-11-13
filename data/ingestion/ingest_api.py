import json
import logging
from pathlib import Path

import boto3
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

API_URL = "https://dummyjson.com/products?limit=0"

BASE_DIR = Path(__file__).resolve().parent.parent
BRONZE_DIR = BASE_DIR / "data_lake" / "bronze"
BRONZE_DIR.mkdir(parents=True, exist_ok=True)

output_file = BRONZE_DIR / "products.json"

# Retry para lidar com instabilidades temporárias da API
retry_strategy = Retry(
    total=2,
    backoff_factor=2,
    status_forcelist=[429, 500, 502, 503, 504, 521],
    allowed_methods=["GET"]
)

adapter = HTTPAdapter(max_retries=retry_strategy)
session = requests.Session()
session.mount("https://", adapter)
session.mount("http://", adapter)

try:

    logger.info("Iniciando ingestão de dados da DummyJSON API.")

    response = session.get(API_URL, timeout=10)
    response.raise_for_status()

    response_data = response.json()

    if not isinstance(response_data, dict):
        raise ValueError("Formato inesperado na resposta da API.")

    data = response_data.get("products")

    if not isinstance(data, list):
        raise ValueError("A API não retornou uma lista válida de produtos.")

    if not data:
        raise ValueError("A API retornou uma lista vazia de produtos.")

    # Validações básicas de qualidade antes de persistir
    required_fields = {"id", "title", "price", "category"}
    seen_ids = set()

    for i, product in enumerate(data):
        missing = required_fields - set(product.keys())
        if missing:
            raise ValueError(f"Produto no índice {i} sem campos obrigatórios: {missing}")

        pid = product["id"]
        if pid in seen_ids:
            raise ValueError(f"ID duplicado encontrado: {pid}")
        seen_ids.add(pid)

        price = product["price"]
        if not isinstance(price, (int, float)) or price <= 0:
            raise ValueError(f"Preço inválido no produto {pid}: {price}")

        if not product["title"] or not str(product["title"]).strip():
            raise ValueError(f"Title vazio no produto {pid}")

        if not product["category"] or not str(product["category"]).strip():
            raise ValueError(f"Category vazia no produto {pid}")

    logger.info(f"Qualidade de dados OK. {len(data)} produtos validados.")

    # Persiste localmente
    with open(output_file, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)

    logger.info(
        f"Ingestão concluída com sucesso. "
        f"Produtos recebidos: {len(data)}. "
        f"Arquivo Bronze: {output_file}"
    )

    # Envia para o Data Lake
    try:
        s3_client = boto3.client("s3")
        bucket_name = "marketplace-data-arlen64-2026"
        s3_key = "bronze/products.json"

        s3_client.upload_file(
            Filename=str(output_file),
            Bucket=bucket_name,
            Key=s3_key
        )

        logger.info(f"Upload para S3 concluído: s3://{bucket_name}/{s3_key}")
    except Exception as s3_error:
        logger.error(f"Erro ao fazer upload para o S3: {s3_error}")
        raise

except requests.exceptions.RequestException as error:
    logger.error(f"Erro ao acessar a API: {error}")
    raise

except (ValueError, json.JSONDecodeError) as error:
    logger.error(f"Erro ao processar os dados da API: {error}")
    raise

except Exception as error:
    logger.error(f"Erro inesperado durante a ingestão: {error}")
    raise

finally:
    session.close()
