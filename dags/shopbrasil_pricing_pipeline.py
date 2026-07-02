from datetime import timedelta

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup

from operators.validar_produtos_operator import ValidarProdutosOperator
from shopbrasil import sql
from shopbrasil.alerts import notificar_falha, notificar_retry, notificar_sucesso
from shopbrasil.api import buscar_produtos_fakestore
from shopbrasil.metrics import calcular_metricas_categoria, extrair_categorias

CONN_ID = "shopbrasil_postgres"
POOL_CATEGORIAS = "ecommerce_pool"


@dag(
    dag_id="shopbrasil_pricing_pipeline",
    description="Métricas diárias de preço por categoria (FakeStore API -> Postgres analítico)",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    default_args={
        "owner": "time-dados",
        "on_failure_callback": notificar_falha,
    },
    tags=["shopbrasil", "pricing", "ecommerce"],
)
def shopbrasil_pricing_pipeline():
    @task(
        retries=3,
        retry_delay=timedelta(seconds=10),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=2),
        on_failure_callback=notificar_falha,
        on_retry_callback=notificar_retry,
        on_success_callback=notificar_sucesso,
    )
    def buscar_produtos() -> list[dict]:
        try:
            produtos = buscar_produtos_fakestore()
        except requests.RequestException as exc:
            raise AirflowException(f"Falha ao consultar a FakeStore API: {exc}") from exc
        if not produtos:
            raise AirflowException("FakeStore API retornou catálogo vazio")
        return produtos

    @task
    def listar_categorias(produtos: list[dict]) -> list[str]:
        return extrair_categorias(produtos)

    @task(pool=POOL_CATEGORIAS)
    def calcular_metricas(produtos: list[dict], categoria: str) -> dict:
        return calcular_metricas_categoria(produtos, categoria)

    @task
    def consolidar_metricas(metricas) -> list[dict]:
        consolidado = sorted(metricas, key=lambda m: m["category"])
        if not consolidado:
            raise AirflowException("Nenhuma métrica foi produzida pelo mapeamento")
        return consolidado

    @task
    def criar_tabelas():
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        hook.run([sql.DDL_SNAPSHOT, sql.DDL_HISTORY])

    @task
    def salvar_metricas(metricas: list[dict], ds=None):
        linhas = sql.montar_linhas_gravacao(ds, metricas)
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        with hook.get_conn() as conexao, conexao.cursor() as cursor:
            cursor.executemany(sql.UPSERT_SNAPSHOT, linhas)
            cursor.executemany(sql.INSERT_HISTORY, linhas)
            conexao.commit()

    with TaskGroup(group_id="ingestao"):
        produtos = buscar_produtos()
        validar = ValidarProdutosOperator(task_id="validar_produtos", produtos=produtos)
        produtos_validos = validar.output

    with TaskGroup(group_id="analise"):
        categorias = listar_categorias(produtos_validos)
        metricas = calcular_metricas.partial(produtos=produtos_validos).expand(
            categoria=categorias
        )
        consolidado = consolidar_metricas(metricas)

    with TaskGroup(group_id="persistencia"):
        tabelas = criar_tabelas()
        gravacao = salvar_metricas(consolidado)
        tabelas >> gravacao


shopbrasil_pricing_pipeline()
