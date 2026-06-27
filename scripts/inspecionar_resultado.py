#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
from datetime import date
from typing import Any

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "database": "shopbrasil",
    "user": "shopbrasil",
    "password": "shopbrasil123",
}


def conectar():
    import psycopg2

    return psycopg2.connect(**DB_CONFIG)


def imprimir_tabela(titulo: str, colunas: list[str], linhas: list[tuple[Any, ...]]) -> None:
    print(f"\n=== {titulo} ===")
    if not linhas:
        print("Sem registros.")
        return

    print(" | ".join(colunas))
    print("-" * 100)
    for linha in linhas:
        print(" | ".join(str(valor) for valor in linha))


def metricas_por_data(conn, data_ref: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                metric_date,
                category,
                preco_medio,
                preco_minimo,
                preco_maximo,
                quantidade_produtos,
                dag_run_id
            FROM product_category_metrics
            WHERE metric_date = %s
            ORDER BY category
            """,
            (data_ref,),
        )
        imprimir_tabela(
            f"METRICAS - {data_ref}",
            [
                "metric_date",
                "category",
                "preco_medio",
                "preco_minimo",
                "preco_maximo",
                "quantidade",
                "dag_run_id",
            ],
            cur.fetchall(),
        )


def duplicidades(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT metric_date, category, COUNT(*) AS total
            FROM product_category_metrics
            GROUP BY metric_date, category
            HAVING COUNT(*) > 1
            ORDER BY metric_date, category
            """
        )
        imprimir_tabela(
            "DUPLICIDADES NA TABELA PRINCIPAL",
            ["metric_date", "category", "total"],
            cur.fetchall(),
        )


def historico(conn, limite: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                metric_date,
                category,
                preco_medio,
                quantidade_produtos,
                dag_run_id,
                inserido_em
            FROM product_category_metrics_history
            ORDER BY inserido_em DESC
            LIMIT %s
            """,
            (limite,),
        )
        imprimir_tabela(
            "HISTORICO RECENTE",
            [
                "metric_date",
                "category",
                "preco_medio",
                "quantidade",
                "dag_run_id",
                "inserido_em",
            ],
            cur.fetchall(),
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspeciona metricas ShopBrasil no PostgreSQL")
    parser.add_argument("--data", default=str(date.today()), help="Data no formato YYYY-MM-DD")
    parser.add_argument("--historico", type=int, default=20, help="Linhas recentes do historico")
    args = parser.parse_args()

    try:
        conn = conectar()
        log.info("Conectado ao banco shopbrasil em localhost:5433")
    except Exception as exc:
        log.error("Nao foi possivel conectar ao banco: %s", exc)
        log.error("Verifique se o ambiente Docker esta rodando com `docker compose ps`.")
        return

    try:
        metricas_por_data(conn, args.data)
        duplicidades(conn)
        historico(conn, args.historico)
    finally:
        conn.close()
        log.info("Inspecao concluida.")


if __name__ == "__main__":
    main()
