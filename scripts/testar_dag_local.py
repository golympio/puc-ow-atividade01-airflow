#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any

FAKESTORE_PRODUCTS_URL = "https://fakestoreapi.com/products"
TMP_PRODUCTS = Path("/tmp/shopbrasil_produtos.json")
TMP_METRICS = Path("/tmp/shopbrasil_metricas.json")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def buscar_produtos() -> list[dict[str, Any]]:
    import requests

    response = requests.get(FAKESTORE_PRODUCTS_URL, timeout=30)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        raise ValueError("FakeStore API retornou payload fora do formato esperado")

    produtos = [
        {
            "id": item.get("id"),
            "title": str(item.get("title", ""))[:200],
            "price": item.get("price"),
            "category": item.get("category"),
        }
        for item in payload
    ]
    log.info("Produtos coletados: %d", len(produtos))
    return produtos


def validar_produtos(produtos: list[dict[str, Any]]) -> None:
    if not produtos:
        raise ValueError("Lista de produtos vazia")

    campos = ("id", "title", "price", "category")
    for indice, produto in enumerate(produtos, start=1):
        ausentes = [campo for campo in campos if produto.get(campo) in (None, "")]
        if ausentes:
            raise ValueError(f"Produto #{indice} sem campos: {ausentes}")
        try:
            Decimal(str(produto["price"]))
        except (InvalidOperation, TypeError) as exc:
            raise ValueError(f"Produto #{indice} com preco invalido") from exc


def listar_categorias(produtos: list[dict[str, Any]]) -> list[str]:
    categorias = sorted({str(produto["category"]).strip() for produto in produtos})
    if not categorias:
        raise ValueError("Nenhuma categoria descoberta")
    log.info("Categorias: %s", ", ".join(categorias))
    return categorias


def money(value: Decimal) -> float:
    return float(value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


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
    return {
        "category": category,
        "preco_medio": money(sum(precos) / Decimal(quantidade)),
        "preco_minimo": money(min(precos)),
        "preco_maximo": money(max(precos)),
        "quantidade_produtos": quantidade,
    }


def calcular_metricas(produtos: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        calcular_metricas_categoria(categoria, produtos)
        for categoria in listar_categorias(produtos)
    ]


def imprimir_metricas(metricas: list[dict[str, Any]]) -> None:
    print("\n=== METRICAS POR CATEGORIA ===")
    for item in metricas:
        print(
            "{category:20s} qtd={quantidade_produtos:2d} "
            "min={preco_minimo:8.2f} media={preco_medio:8.2f} max={preco_maximo:8.2f}".format(
                **item
            )
        )
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Testa a logica da DAG ShopBrasil sem Airflow")
    parser.add_argument(
        "--step",
        choices=["fetch", "metrics", "all"],
        default="all",
        help="Etapa local a executar",
    )
    args = parser.parse_args()

    produtos: list[dict[str, Any]]
    if args.step in ("fetch", "all"):
        produtos = buscar_produtos()
        validar_produtos(produtos)
        TMP_PRODUCTS.write_text(json.dumps(produtos, indent=2), encoding="utf-8")
        log.info("Produtos salvos em %s", TMP_PRODUCTS)
    else:
        produtos = json.loads(TMP_PRODUCTS.read_text(encoding="utf-8"))
        validar_produtos(produtos)

    if args.step in ("metrics", "all"):
        metricas = calcular_metricas(produtos)
        imprimir_metricas(metricas)
        TMP_METRICS.write_text(json.dumps(metricas, indent=2), encoding="utf-8")
        log.info("Metricas salvas em %s", TMP_METRICS)

    log.info("Teste local concluido.")


if __name__ == "__main__":
    main()
