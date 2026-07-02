"""Funções puras de cálculo de métricas de pricing."""


def extrair_categorias(produtos: list[dict]) -> list[str]:
    """Extrai as categorias únicas presentes nos produtos, em ordem alfabética."""
    return sorted({produto["category"] for produto in produtos})


def calcular_metricas_categoria(produtos: list[dict], categoria: str) -> dict:
    """Calcula as métricas de preço de uma categoria."""
    precos = [float(produto["price"]) for produto in produtos if produto["category"] == categoria]
    if not precos:
        raise ValueError(f"Nenhum produto encontrado para a categoria '{categoria}'")
    return {
        "category": categoria,
        "avg_price": round(sum(precos) / len(precos), 2),
        "min_price": min(precos),
        "max_price": max(precos),
        "product_count": len(precos),
    }
