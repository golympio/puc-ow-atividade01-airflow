# Atividade 1 - Airflow ShopBrasil

Projeto da disciplina OW para substituir um cron fragil por uma DAG em Apache Airflow. A
DAG coleta produtos da FakeStore API, calcula metricas de preco por categoria e persiste um
snapshot idempotente em PostgreSQL.

## Estrutura

```text
puc-ow-atividade01-airflow/
├── docker-compose.yml
├── dags/
│   └── shopbrasil_precos_categoria.py
├── plugins/
│   └── validar_produtos_operator.py
├── sql/
│   └── init.sql
└── README.md
```

## Repositorio de entrega

```bash
git clone https://github.com/golympio/puc-ow-atividade01-airflow.git
cd puc-ow-atividade01-airflow
```

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

## Requisitos atendidos

| Requisito do enunciado | Implementacao |
|---|---|
| Projeto dockerizado | `docker-compose.yml` sobe Airflow 2.9.3, banco de metadados e PostgreSQL analitico. |
| Fonte FakeStore API | A DAG coleta produtos em `https://fakestoreapi.com/products`. |
| TaskFlow API | A DAG usa `@dag` e `@task`, com dependencias por chamada de funcao. |
| XCom automatico | As tasks passam listas pequenas via `return`, sem arquivos intermediarios obrigatorios. |
| Topologias linear, fan-out e fan-in | Fluxo linear entre ingestao, validacao, categorias, metricas e persistencia; fan-out por categoria com `.expand(...)`; fan-in em `consolidar_metricas`. |
| Agendamento diario as 06:00 | `schedule="0 6 * * *"`, `pendulum`, timezone `America/Sao_Paulo`, `start_date` e `catchup=False`. |
| Ingestao resiliente | Task `buscar_produtos` tem retry, exponential backoff, timeout, `try/except` e `raise`. |
| Callbacks de ciclo de vida | `on_success_callback`, `on_retry_callback` e `on_failure_callback` registram eventos estruturados nos logs. |
| Processamento paralelo | `calcular_metricas_categoria` usa Dynamic Task Mapping por categoria. |
| Pool de concorrencia | Pool `ecommerce_pool` com 2 slots, configurado no `airflow-init`. |
| TaskGroups | A DAG possui `ingestao`, `analise` e `persistencia`. |
| Persistencia no PostgreSQL | `persistir_metricas` usa `PostgresHook` com a Connection `postgres_shopbrasil`. |
| Idempotencia | `product_category_metrics` usa chave `(metric_date, category)` e `ON CONFLICT DO UPDATE`. |
| Operador customizado opcional | `plugins/validar_produtos_operator.py` implementa `ValidarProdutosOperator(BaseOperator)`. |
| Historico opcional | `product_category_metrics_history` grava historico append-only por execucao. |
| SLA/alerta opcional | `on_failure_callback` simula alerta com `shopbrasil_alerta_simulado`. |

## Comandos principais

No diretorio do repositorio:

```bash
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


## Validacao da persistencia e idempotencia

Com o ambiente rodando e a DAG executada com sucesso, consulte diretamente o banco
analitico:

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
- A consulta em `v_product_category_metrics_latest` lista metricas por categoria.
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
| Banco sem dados | Conferir execucao da DAG e consultar `v_product_category_metrics_latest` |
