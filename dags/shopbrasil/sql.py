"""DDL e statements de gravação na base analítica."""

DDL_SNAPSHOT = """
CREATE TABLE IF NOT EXISTS pricing_category_snapshot (
    execution_date DATE NOT NULL,
    category TEXT NOT NULL,
    avg_price NUMERIC(12, 2) NOT NULL,
    min_price NUMERIC(12, 2) NOT NULL,
    max_price NUMERIC(12, 2) NOT NULL,
    product_count INTEGER NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (execution_date, category)
);
"""

DDL_HISTORY = """
CREATE TABLE IF NOT EXISTS pricing_category_history (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    execution_date DATE NOT NULL,
    category TEXT NOT NULL,
    avg_price NUMERIC(12, 2) NOT NULL,
    min_price NUMERIC(12, 2) NOT NULL,
    max_price NUMERIC(12, 2) NOT NULL,
    product_count INTEGER NOT NULL,
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW()
);
"""

UPSERT_SNAPSHOT = """
INSERT INTO pricing_category_snapshot
    (execution_date, category, avg_price, min_price, max_price, product_count, updated_at)
VALUES (%s, %s, %s, %s, %s, %s, NOW())
ON CONFLICT (execution_date, category) DO UPDATE SET
    avg_price = EXCLUDED.avg_price,
    min_price = EXCLUDED.min_price,
    max_price = EXCLUDED.max_price,
    product_count = EXCLUDED.product_count,
    updated_at = NOW();
"""

INSERT_HISTORY = """
INSERT INTO pricing_category_history
    (execution_date, category, avg_price, min_price, max_price, product_count)
VALUES (%s, %s, %s, %s, %s, %s);
"""


def montar_linhas_gravacao(execution_date: str, metricas: list[dict]) -> list[tuple]:
    """Monta as tuplas de parâmetros do upsert a partir das métricas consolidadas."""
    return [
        (
            execution_date,
            metrica["category"],
            metrica["avg_price"],
            metrica["min_price"],
            metrica["max_price"],
            metrica["product_count"],
        )
        for metrica in metricas
    ]
