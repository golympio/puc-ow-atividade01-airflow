from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Sequence

from airflow.models import BaseOperator


class ValidarProdutosOperator(BaseOperator):
    """Valida o schema minimo dos produtos retornados pela FakeStore API."""

    template_fields: Sequence[str] = ("produtos",)

    def __init__(
        self,
        *,
        produtos: list[dict[str, Any]],
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.produtos = produtos

    def execute(self, context: dict[str, Any]) -> int:
        produtos = self.produtos
        if not isinstance(produtos, list) or not produtos:
            raise ValueError("A lista de produtos esta vazia ou invalida")

        campos_obrigatorios = ("id", "title", "price", "category")
        categorias = set()

        for indice, produto in enumerate(produtos, start=1):
            if not isinstance(produto, dict):
                raise ValueError(f"Produto #{indice} nao e um objeto valido")

            ausentes = [
                campo
                for campo in campos_obrigatorios
                if produto.get(campo) in (None, "")
            ]
            if ausentes:
                raise ValueError(f"Produto #{indice} sem campos obrigatorios: {ausentes}")

            try:
                preco = Decimal(str(produto["price"]))
            except (InvalidOperation, TypeError) as exc:
                raise ValueError(f"Produto #{indice} tem preco nao numerico") from exc

            if preco < 0:
                raise ValueError(f"Produto #{indice} tem preco negativo")

            categorias.add(str(produto["category"]).strip())

        self.log.info(
            "Validacao de produtos concluida: produtos=%d, categorias=%d",
            len(produtos),
            len(categorias),
        )
        return len(produtos)
