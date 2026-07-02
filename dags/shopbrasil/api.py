"""Cliente da FakeStore API."""

import requests

FAKESTORE_PRODUCTS_URL = "https://fakestoreapi.com/products"
CAMPOS_PRODUTO = ("id", "title", "price", "category")


def buscar_produtos_fakestore(timeout: int = 30) -> list[dict]:
    """Busca o catálogo de produtos e retorna apenas os campos usados no pipeline."""
    resposta = requests.get(FAKESTORE_PRODUCTS_URL, timeout=timeout)
    resposta.raise_for_status()
    produtos = resposta.json()
    return [{campo: produto.get(campo) for campo in CAMPOS_PRODUTO} for produto in produtos]
