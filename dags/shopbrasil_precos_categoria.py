from __future__ import annotations

import json
import logging
from datetime import timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

import pendulum
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup

from validar_produtos_operator import ValidarProdutosOperator


log = logging.getLogger(__name__)

FAKESTORE_PRODUCTS_URL = "https://fakestoreapi.com/products"
POSTGRES_CONN_ID = "postgres_shopbrasil"
LOCAL_TZ = "America/Sao_Paulo"

DEFAULT_ARGS = {
    "owner": "shopbrasil",
    "email_on_failure": False,
    "email_on_retry": False,
}


def _callback_payload(context: dict[str, Any], status: str) -> dict[str, str]:
    task = context.get("task")
    dag_run = context.get("dag_run")
    exception = context.get("exception")
    return {
        "status": status,
        "dag_id": getattr(task, "dag_id", context.get("dag_id", "unknown")),
        "task_id": getattr(task, "task_id", "unknown"),
        "run_id": getattr(dag_run, "run_id", context.get("run_id", "unknown")),
        "exception": str(exception) if exception else "",
    }


def on_success_callback(context: dict[str, Any]) -> None:
    payload = _callback_payload(context, "success")
    log.info("shopbrasil_task_event=%s", json.dumps(payload, sort_keys=True))


def on_retry_callback(context: dict[str, Any]) -> None:
    payload = _callback_payload(context, "retry")
    log.warning("shopbrasil_task_event=%s", json.dumps(payload, sort_keys=True))


def on_failure_callback(context: dict[str, Any]) -> None:
    payload = _callback_payload(context, "failure")
    log.error("shopbrasil_alerta_simulado=%s", json.dumps(payload, sort_keys=True))


def _money(value: Decimal) -> float:
    return float(value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


@dag(
    dag_id="shopbrasil_precos_categoria",
    description="Metricas de preco por categoria da FakeStore API para a ShopBrasil",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 6, 1, tz=LOCAL_TZ),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["shopbrasil", "fakestore", "atividade-1", "airflow"],
    max_active_runs=1,
)
def shopbrasil_precos_categoria():
    @task(
        task_id="buscar_produtos",
        retries=3,
        retry_delay=timedelta(minutes=1),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=15),
        execution_timeout=timedelta(minutes=2),
        on_success_callback=on_success_callback,
        on_failure_callback=on_failure_callback,
        on_retry_callback=on_retry_callback,
    )
    def buscar_produtos() -> list[dict[str, Any]]:
        import requests

        try:
            response = requests.get(FAKESTORE_PRODUCTS_URL, timeout=30)
            response.raise_for_status()
            payload = response.json()

            if not isinstance(payload, list):
                raise ValueError("FakeStore API retornou payload fora do formato esperado")

            produtos = []
            for item in payload:
                produtos.append(
                    {
                        "id": item.get("id"),
                        "title": str(item.get("title", ""))[:200],
                        "price": item.get("price"),
                        "category": item.get("category"),
                    }
                )

            log.info("Produtos coletados da FakeStore API: %d", len(produtos))
            return produtos

        except Exception:
            log.exception("Falha ao buscar produtos em %s", FAKESTORE_PRODUCTS_URL)
            raise

    @task(task_id="listar_categorias")
    def listar_categorias(produtos: list[dict[str, Any]]) -> list[str]:
        categorias = sorted(
            {
                str(produto["category"]).strip()
                for produto in produtos
                if produto.get("category")
            }
        )
        if not categorias:
            raise ValueError("Nenhuma categoria encontrada nos produtos")

        log.info("Categorias descobertas dinamicamente: %s", categorias)
        return categorias

    @task(
        task_id="calcular_metricas_categoria",
        pool="ecommerce_pool",
        pool_slots=1,
    )
    def calcular_metricas_categoria(
        category: str,
        produtos: list[dict[str, Any]],
    ) -> dict[str, Any]:
        precos = [
            Decimal(str(produto["price"]))
            for produto in produtos
            if produto.get("category") == category
        ]
        if not precos:
            raise ValueError(f"Categoria sem produtos: {category}")

        quantidade = len(precos)
        preco_minimo = min(precos)
        preco_maximo = max(precos)
        preco_medio = sum(precos) / Decimal(quantidade)

        metricas = {
            "category": category,
            "preco_medio": _money(preco_medio),
            "preco_minimo": _money(preco_minimo),
            "preco_maximo": _money(preco_maximo),
            "quantidade_produtos": quantidade,
        }
        log.info("Metricas calculadas para %s: %s", category, metricas)
        return metricas

    @task(task_id="consolidar_metricas")
    def consolidar_metricas(metricas_por_categoria: list[dict[str, Any]]) -> list[dict[str, Any]]:
        metricas = sorted(metricas_por_categoria, key=lambda item: item["category"])
        log.info("Metricas consolidadas para %d categorias", len(metricas))
        return metricas

    @task(task_id="persistir_metricas")
    def persistir_metricas(metricas: list[dict[str, Any]], **context: Any) -> int:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        metric_date = context["ds"]
        run_id = context["run_id"]
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()

        upsert_sql = """
            INSERT INTO product_category_metrics (
                metric_date,
                category,
                preco_medio,
                preco_minimo,
                preco_maximo,
                quantidade_produtos,
                dag_run_id,
                source_endpoint,
                atualizado_em
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (metric_date, category)
            DO UPDATE SET
                preco_medio = EXCLUDED.preco_medio,
                preco_minimo = EXCLUDED.preco_minimo,
                preco_maximo = EXCLUDED.preco_maximo,
                quantidade_produtos = EXCLUDED.quantidade_produtos,
                dag_run_id = EXCLUDED.dag_run_id,
                source_endpoint = EXCLUDED.source_endpoint,
                atualizado_em = NOW()
        """
        history_sql = """
            INSERT INTO product_category_metrics_history (
                metric_date,
                category,
                preco_medio,
                preco_minimo,
                preco_maximo,
                quantidade_produtos,
                dag_run_id,
                source_endpoint
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        valores = [
            (
                metric_date,
                item["category"],
                item["preco_medio"],
                item["preco_minimo"],
                item["preco_maximo"],
                item["quantidade_produtos"],
                run_id,
                FAKESTORE_PRODUCTS_URL,
            )
            for item in metricas
        ]

        try:
            cur.executemany(upsert_sql, valores)
            cur.executemany(history_sql, valores)
            conn.commit()
            log.info("%d categorias persistidas para metric_date=%s", len(valores), metric_date)
            return len(valores)

        except Exception:
            conn.rollback()
            log.exception("Rollback executado ao persistir metricas")
            raise

        finally:
            cur.close()
            conn.close()

    with TaskGroup("ingestao") as ingestao:
        produtos = buscar_produtos()
        validar_produtos = ValidarProdutosOperator(
            task_id="validar_produtos",
            produtos=produtos,
        )

    with TaskGroup("analise") as analise:
        categorias = listar_categorias(produtos)
        metricas_mapeadas = calcular_metricas_categoria.partial(
            produtos=produtos,
        ).expand(
            category=categorias,
        )
        metricas_consolidadas = consolidar_metricas(metricas_mapeadas)

    with TaskGroup("persistencia") as persistencia:
        persistir_metricas(metricas_consolidadas)

    produtos >> validar_produtos >> categorias
    ingestao >> analise >> persistencia


dag_instance = shopbrasil_precos_categoria()
