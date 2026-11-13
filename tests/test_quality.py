import pytest


def validate_products(data):
    """Validação básica de produtos (espelha a lógica do ingest_api.py)."""
    if not isinstance(data, list) or not data:
        raise ValueError("Lista de produtos inválida ou vazia")

    required_fields = {"id", "title", "price", "category"}
    seen_ids = set()

    for i, product in enumerate(data):
        missing = required_fields - set(product.keys())
        if missing:
            raise ValueError(f"Campos obrigatórios ausentes no índice {i}: {missing}")

        pid = product["id"]
        if pid in seen_ids:
            raise ValueError(f"ID duplicado encontrado: {pid}")
        seen_ids.add(pid)

        price = product["price"]
        if not isinstance(price, (int, float)) or price <= 0:
            raise ValueError(f"Preço inválido no produto {pid}: {price}")

        if not str(product.get("title", "")).strip():
            raise ValueError(f"Title vazio no produto {pid}")

        if not str(product.get("category", "")).strip():
            raise ValueError(f"Category vazia no produto {pid}")

    return True


def test_produtos_validos():
    data = [
        {"id": 1, "title": "Produto A", "price": 10.5, "category": "eletronicos"},
        {"id": 2, "title": "Produto B", "price": 25, "category": "casa"},
    ]
    assert validate_products(data) is True


def test_lista_vazia():
    with pytest.raises(ValueError, match="vazia"):
        validate_products([])


def test_id_duplicado():
    data = [
        {"id": 1, "title": "A", "price": 10, "category": "x"},
        {"id": 1, "title": "B", "price": 20, "category": "x"},
    ]
    with pytest.raises(ValueError, match="duplicado"):
        validate_products(data)


def test_preco_invalido():
    data = [{"id": 1, "title": "A", "price": 0, "category": "x"}]
    with pytest.raises(ValueError, match="Preço inválido"):
        validate_products(data)


def test_campo_obrigatorio_ausente():
    data = [{"id": 1, "title": "A", "price": 10}]  # falta category
    with pytest.raises(ValueError, match="obrigatórios"):
        validate_products(data)