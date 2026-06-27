-- =============================================================================
-- Atividade 1 - ShopBrasil
-- Inicializacao do PostgreSQL analitico (postgres-shopbrasil)
-- =============================================================================

CREATE TABLE IF NOT EXISTS product_category_metrics (
    metric_date          DATE NOT NULL,
    category             VARCHAR(120) NOT NULL,
    preco_medio          NUMERIC(12, 2) NOT NULL,
    preco_minimo         NUMERIC(12, 2) NOT NULL,
    preco_maximo         NUMERIC(12, 2) NOT NULL,
    quantidade_produtos  INTEGER NOT NULL,
    dag_run_id           VARCHAR(250),
    source_endpoint      TEXT NOT NULL DEFAULT 'https://fakestoreapi.com/products',
    atualizado_em        TIMESTAMP NOT NULL DEFAULT NOW(),
    criado_em            TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT product_category_metrics_pk PRIMARY KEY (metric_date, category),
    CONSTRAINT product_category_metrics_quantidade_chk CHECK (quantidade_produtos >= 0),
    CONSTRAINT product_category_metrics_precos_chk CHECK (
        preco_minimo <= preco_medio
        AND preco_medio <= preco_maximo
    )
);

CREATE INDEX IF NOT EXISTS idx_product_category_metrics_category
    ON product_category_metrics (category);

CREATE TABLE IF NOT EXISTS product_category_metrics_history (
    id                   BIGSERIAL PRIMARY KEY,
    metric_date          DATE NOT NULL,
    category             VARCHAR(120) NOT NULL,
    preco_medio          NUMERIC(12, 2) NOT NULL,
    preco_minimo         NUMERIC(12, 2) NOT NULL,
    preco_maximo         NUMERIC(12, 2) NOT NULL,
    quantidade_produtos  INTEGER NOT NULL,
    dag_run_id           VARCHAR(250),
    source_endpoint      TEXT NOT NULL DEFAULT 'https://fakestoreapi.com/products',
    inserido_em          TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_product_category_metrics_history_date_category
    ON product_category_metrics_history (metric_date, category);

CREATE OR REPLACE VIEW v_product_category_metrics_latest AS
SELECT
    metric_date,
    category,
    preco_medio,
    preco_minimo,
    preco_maximo,
    quantidade_produtos,
    dag_run_id,
    atualizado_em
FROM product_category_metrics
ORDER BY metric_date DESC, category;

CREATE OR REPLACE VIEW v_product_category_metrics_duplicates AS
SELECT
    metric_date,
    category,
    COUNT(*) AS total
FROM product_category_metrics
GROUP BY metric_date, category
HAVING COUNT(*) > 1;

GRANT ALL ON ALL TABLES IN SCHEMA public TO shopbrasil;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO shopbrasil;
GRANT SELECT ON v_product_category_metrics_latest TO shopbrasil;
GRANT SELECT ON v_product_category_metrics_duplicates TO shopbrasil;

DO $$ BEGIN
    RAISE NOTICE 'Banco shopbrasil inicializado com sucesso.';
    RAISE NOTICE 'Tabelas: product_category_metrics, product_category_metrics_history.';
    RAISE NOTICE 'Views: v_product_category_metrics_latest, v_product_category_metrics_duplicates.';
END $$;
