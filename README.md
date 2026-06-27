# Atividade 1 - Airflow ShopBrasil

Projeto da disciplina OW para substituir um cron fragil por uma DAG em Apache Airflow. A
DAG coleta produtos da FakeStore API, calcula metricas de preco por categoria e persiste um
snapshot idempotente em PostgreSQL.

## Estrutura

```text
atividade01-airflow/
├── docker-compose.yml
├── dags/
│   └── shopbrasil_precos_categoria.py
├── plugins/
│   └── validar_produtos_operator.py
├── scripts/
│   ├── testar_dag_local.py
│   └── inspecionar_resultado.py
├── sql/
│   └── init.sql
└── README.md
```

O diretorio `OW/lab/lab01-airflow/` foi usado apenas como referencia e nao deve ser
alterado para esta entrega.

## Ambiente

| Item | Valor |
|---|---|
| Airflow | `apache/airflow:2.9.3-python3.11` |
| UI | `http://localhost:8080` |
| Usuario Airflow | `admin` |
| Senha Airflow | `admin` |
| Connection Airflow | `postgres_shopbrasil` |
| Pool Airflow | `ecommerce_pool` com 2 slots |
| Banco analitico | `shopbrasil` |
| Usuario do banco | `shopbrasil` |
| Senha do banco | `shopbrasil123` |
| Porta local do banco | `5433` |

As credenciais acima sao apenas de laboratorio local.

## Comandos principais

```bash
cd atividade01-airflow

# Validar a configuracao sem iniciar containers
docker compose config

# Subir o ambiente local
docker compose up -d

# Conferir os containers
docker compose ps

# Acompanhar logs do scheduler
docker compose logs -f airflow-scheduler

# Parar preservando volumes/dados
docker compose down
```

Para apagar volumes e dados locais, use `docker compose down -v` somente depois de
confirmar explicitamente que a perda dos dados de laboratorio e aceitavel.

## Validacao no Airflow

1. Acesse `http://localhost:8080`.
2. Entre com `admin` / `admin`.
3. Confirme que a DAG `shopbrasil_precos_categoria` aparece sem erro de importacao.
4. Confirme em Admin > Connections a Connection `postgres_shopbrasil`.
5. Confirme em Admin > Pools o pool `ecommerce_pool` com 2 slots.
6. Acione uma execucao manual da DAG.
7. Confira no grafo os TaskGroups, o trecho linear, o fan-out mapeado por categoria e o
   fan-in antes da persistencia.

As validacoes com Docker rodando e Airflow UI devem ser feitas ou confirmadas pelo humano
responsavel na etapa final da entrega.

## Inspecao dos resultados

Com o ambiente rodando, execute:

```bash
python scripts/inspecionar_resultado.py
```

Ou consulte diretamente o banco analitico:

```bash
docker compose exec postgres-shopbrasil psql -U shopbrasil -d shopbrasil -c "SELECT * FROM v_product_category_metrics_latest;"
```

Consulta de idempotencia esperada:

```sql
SELECT metric_date, category, COUNT(*) AS total
FROM product_category_metrics
GROUP BY metric_date, category
HAVING COUNT(*) > 1;
```

O resultado deve retornar zero linhas, inclusive apos reprocessar a mesma data.

## Teste local da logica

O script abaixo testa fetch, validacao e metricas sem iniciar o Airflow:

```bash
python scripts/testar_dag_local.py
```

Esse teste usa rede externa para chamar a FakeStore API. Se a API estiver indisponivel, a
DAG e o script devem falhar explicitamente em vez de gerar metricas incompletas.

## Callback de falha

Para validar callbacks sem enviar alertas externos reais, force uma falha controlada
alterando temporariamente o endpoint da FakeStore API na DAG para uma URL invalida,
execute a DAG manualmente e observe os logs do scheduler:

```bash
docker compose logs airflow-scheduler
```

Depois restaure o endpoint correto e execute novamente a DAG ate sucesso.

## Checklist de evidencias

- `docker compose config` executa com sucesso.
- `docker compose ps` mostra os servicos principais sem estado unhealthy.
- A UI em `http://localhost:8080` mostra somente a DAG `shopbrasil_precos_categoria`.
- A DAG nao tem erro de importacao.
- A Connection `postgres_shopbrasil` existe.
- O pool `ecommerce_pool` existe com 2 slots.
- Uma execucao manual da DAG termina com todas as tasks em sucesso.
- O grafo mostra TaskGroups, trecho linear, fan-out mapeado e fan-in.
- `python scripts/inspecionar_resultado.py` lista metricas por categoria.
- A consulta de duplicidade retorna zero linhas.
- Logs do scheduler mostram callback de sucesso e, em simulacao controlada, retry/falha
  com `shopbrasil_task_event` ou `shopbrasil_alerta_simulado`.

## Troubleshooting

| Problema | Verificacao |
|---|---|
| UI nao abre | `docker compose ps` e logs do `airflow-webserver` |
| DAG nao aparece | `docker compose logs airflow-scheduler` |
| Erro de importacao | Conferir `dags/shopbrasil_precos_categoria.py` e plugins |
| Connection ausente | Conferir logs do `airflow-init` |
| Pool ausente | Conferir logs do `airflow-init` e Admin > Pools |
| Banco sem dados | Conferir execucao da DAG e `scripts/inspecionar_resultado.py` |

## Entrega

Antes de publicar, fazer push, criar repositorio remoto ou postar link externo, pare e
solicite aprovacao explicita com comandos, impacto, validacao e rollback.
