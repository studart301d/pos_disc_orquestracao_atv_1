set -euo pipefail

airflow db migrate

airflow users create \
    --username airflow \
    --password airflow \
    --firstname Admin \
    --lastname ShopBrasil \
    --role Admin \
    --email admin@shopbrasil.local || true

if ! airflow connections get shopbrasil_postgres >/dev/null 2>&1; then
    airflow connections add shopbrasil_postgres \
        --conn-type postgres \
        --conn-host postgres-analytics \
        --conn-port 5432 \
        --conn-login shopbrasil \
        --conn-password shopbrasil \
        --conn-schema shopbrasil
fi

airflow pools set ecommerce_pool 2 "Limita a concorrencia das tasks mapeadas por categoria"

echo "Inicializacao concluida."
