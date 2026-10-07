import json
import logging
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# Configura o sistema de logs para registrar informações, avisos e erros durante a execução
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# Cria o logger utilizado pelo script
logger = logging.getLogger(__name__)


# Endpoint da DummyJSON que retorna todos os produtos
API_URL = "https://dummyjson.com/products?limit=0"


# Define o diretório base do projeto e a pasta da camada Bronze
BASE_DIR = Path(__file__).resolve().parent.parent
BRONZE_DIR = BASE_DIR / "data_lake" / "bronze"

# Garante que o diretório Bronze exista
BRONZE_DIR.mkdir(parents=True, exist_ok=True)

# Arquivo de destino dos dados brutos
output_file = BRONZE_DIR / "products.json"


# Configura tentativas automáticas para falhas temporárias da API
retry_strategy = Retry(
    total=2,
    backoff_factor=2,
    status_forcelist=[429, 500, 502, 503, 504, 521],
    allowed_methods=["GET"]
)

# Associa a estratégia de retry às requisições HTTP/HTTPS
adapter = HTTPAdapter(max_retries=retry_strategy)

session = requests.Session()
session.mount("https://", adapter)
session.mount("http://", adapter)


try:
    # Registra o início da ingestão
    logger.info("Iniciando ingestão de dados da DummyJSON API.")

    # Realiza a requisição utilizando retry automático
    response = session.get(API_URL, timeout=10)

    # Gera erro caso a API retorne status HTTP inválido
    response.raise_for_status()

    # Converte a resposta da API para JSON
    response_data = response.json()

    # Valida o formato da resposta
    if not isinstance(response_data, dict):
        raise ValueError(
            "Formato inesperado na resposta da API."
        )

    # Extrai a lista de produtos
    data = response_data.get("products")

    
    
    
    # Valida se a API retornou uma lista de produtos
    if not isinstance(data, list):
        raise ValueError(
            "A API não retornou uma lista válida de produtos."
        )

    # Impede que o pipeline continue caso a API não retorne produtos
    if not data:
        raise ValueError(
            "A API retornou uma lista vazia de produtos."
        )

    # Salva os dados brutos na camada Bronze
    with open(output_file, "w", encoding="utf-8") as file:
        json.dump(
            data,
            file,
            ensure_ascii=False,
            indent=2
        )

    logger.info(
        f"Ingestão concluída com sucesso. "
        f"Produtos recebidos: {len(data)}. "
        f"Arquivo Bronze: {output_file}"
    )

except requests.exceptions.RequestException as error:
    # Erros relacionados à comunicação com a API
    logger.error(f"Erro ao acessar a API: {error}")
    raise

except (ValueError, json.JSONDecodeError) as error:
    # Erros relacionados ao conteúdo recebido
    logger.error(f"Erro ao processar os dados da API: {error}")
    raise

except Exception as error:
    # Captura qualquer outro erro inesperado
    logger.error(f"Erro inesperado durante a ingestão: {error}")
    raise

finally:
    # Fecha a sessão HTTP ao final da execução
    session.close()