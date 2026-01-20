-- ============================================================
-- Queries Athena - Tech Challenge Fase 2
-- Pipeline Batch Bovespa
-- ============================================================
-- Estas queries são exemplos para validação dos dados refinados
-- Database: b3_database
-- Tabela: refined_b3_data
-- ============================================================


-- ============================================================
-- 1. QUERIES DE VALIDAÇÃO BÁSICA
-- ============================================================

-- 1.1 Verificar se a tabela existe e tem dados
SELECT COUNT(*) as total_registros
FROM b3_database.refined_b3_data;


-- 1.2 Visualizar amostra dos dados
SELECT *
FROM b3_database.refined_b3_data
LIMIT 10;


-- 1.3 Verificar schema da tabela
DESCRIBE b3_database.refined_b3_data;


-- 1.4 Verificar partições disponíveis
SHOW PARTITIONS b3_database.refined_b3_data;


-- ============================================================
-- 2. QUERIES DE AGREGAÇÃO POR TICKER
-- ============================================================

-- 2.1 Volume total e preço médio por ticker
SELECT
    ticker,
    COUNT(*) as dias_negociados,
    SUM(volume_total_diario) as volume_total,
    AVG(preco_fechamento) as preco_medio,
    MIN(preco_minimo) as minima_historica,
    MAX(preco_maximo) as maxima_historica
FROM b3_database.refined_b3_data
GROUP BY ticker
ORDER BY volume_total DESC;


-- 2.2 Top 5 ações por volume negociado
SELECT
    ticker,
    SUM(volume_total_diario) as volume_total
FROM b3_database.refined_b3_data
GROUP BY ticker
ORDER BY volume_total DESC
LIMIT 5;


-- 2.3 Preço médio de fechamento por ticker
SELECT
    ticker,
    ROUND(AVG(preco_fechamento), 2) as preco_medio_fechamento,
    ROUND(AVG(preco_medio_fechamento), 2) as media_das_medias
FROM b3_database.refined_b3_data
GROUP BY ticker
ORDER BY ticker;


-- ============================================================
-- 3. QUERIES TEMPORAIS
-- ============================================================

-- 3.1 Evolução diária por ticker
SELECT
    data_particao,
    ticker,
    preco_fechamento,
    media_movel_7d,
    variacao_percentual_diaria
FROM b3_database.refined_b3_data
WHERE ticker = 'PETR4'
ORDER BY data_particao DESC
LIMIT 30;


-- 3.2 Média móvel de 7 dias vs preço de fechamento
SELECT
    data_particao,
    ticker,
    preco_fechamento,
    media_movel_7d,
    ROUND(preco_fechamento - media_movel_7d, 2) as diferenca_media
FROM b3_database.refined_b3_data
WHERE ticker = 'VALE3'
ORDER BY data_particao DESC;


-- 3.3 Variação percentual diária
SELECT
    data_particao,
    ticker,
    preco_fechamento,
    ROUND(variacao_percentual_diaria, 2) as variacao_pct
FROM b3_database.refined_b3_data
ORDER BY ABS(variacao_percentual_diaria) DESC
LIMIT 20;


-- 3.4 Dias com maior volatilidade (maior variação)
SELECT
    data_particao,
    ticker,
    preco_fechamento,
    variacao_percentual_diaria
FROM b3_database.refined_b3_data
WHERE ABS(variacao_percentual_diaria) > 5
ORDER BY ABS(variacao_percentual_diaria) DESC;


-- ============================================================
-- 4. QUERIES DE ANÁLISE DE PERÍODO
-- ============================================================

-- 4.1 Máximas e mínimas do período por ticker
SELECT
    ticker,
    MIN(preco_minimo) as minima_periodo,
    MAX(preco_maximo) as maxima_periodo,
    ROUND((MAX(preco_maximo) - MIN(preco_minimo)) / MIN(preco_minimo) * 100, 2) as amplitude_pct
FROM b3_database.refined_b3_data
GROUP BY ticker
ORDER BY amplitude_pct DESC;


-- 4.2 Performance acumulada no período
SELECT
    ticker,
    MIN(data_particao) as data_inicio,
    MAX(data_particao) as data_fim,
    FIRST_VALUE(preco_fechamento) OVER (PARTITION BY ticker ORDER BY data_particao) as preco_inicial,
    LAST_VALUE(preco_fechamento) OVER (PARTITION BY ticker ORDER BY data_particao
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as preco_final
FROM b3_database.refined_b3_data
GROUP BY ticker, data_particao, preco_fechamento;


-- 4.3 Resumo do período por ticker
WITH periodo AS (
    SELECT
        ticker,
        MIN(data_particao) as data_inicio,
        MAX(data_particao) as data_fim
    FROM b3_database.refined_b3_data
    GROUP BY ticker
),
primeiro_preco AS (
    SELECT DISTINCT
        ticker,
        FIRST_VALUE(preco_fechamento) OVER (PARTITION BY ticker ORDER BY data_particao) as preco_inicial
    FROM b3_database.refined_b3_data
),
ultimo_preco AS (
    SELECT DISTINCT
        ticker,
        LAST_VALUE(preco_fechamento) OVER (PARTITION BY ticker ORDER BY data_particao
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as preco_final
    FROM b3_database.refined_b3_data
)
SELECT
    p.ticker,
    p.data_inicio,
    p.data_fim,
    ROUND(pp.preco_inicial, 2) as preco_inicial,
    ROUND(up.preco_final, 2) as preco_final,
    ROUND((up.preco_final - pp.preco_inicial) / pp.preco_inicial * 100, 2) as retorno_pct
FROM periodo p
JOIN primeiro_preco pp ON p.ticker = pp.ticker
JOIN ultimo_preco up ON p.ticker = up.ticker
ORDER BY retorno_pct DESC;


-- ============================================================
-- 5. QUERIES DE QUALIDADE DOS DADOS
-- ============================================================

-- 5.1 Verificar dados válidos vs inválidos
SELECT
    dados_validos,
    COUNT(*) as quantidade
FROM b3_database.refined_b3_data
GROUP BY dados_validos;


-- 5.2 Verificar registros por data de processamento
SELECT
    DATE(data_processamento) as data_proc,
    COUNT(*) as registros_processados
FROM b3_database.refined_b3_data
GROUP BY DATE(data_processamento)
ORDER BY data_proc DESC;


-- 5.3 Verificar cobertura de datas por ticker
SELECT
    ticker,
    COUNT(DISTINCT data_particao) as dias_com_dados,
    MIN(data_particao) as primeira_data,
    MAX(data_particao) as ultima_data
FROM b3_database.refined_b3_data
GROUP BY ticker
ORDER BY ticker;


-- 5.4 Identificar gaps nos dados (dias sem negociação)
WITH datas AS (
    SELECT DISTINCT data_particao
    FROM b3_database.refined_b3_data
    ORDER BY data_particao
),
sequencia AS (
    SELECT
        data_particao,
        LAG(data_particao) OVER (ORDER BY data_particao) as data_anterior
    FROM datas
)
SELECT
    data_anterior as data_inicio_gap,
    data_particao as data_fim_gap,
    DATE_DIFF('day', CAST(data_anterior AS DATE), CAST(data_particao AS DATE)) as dias_gap
FROM sequencia
WHERE DATE_DIFF('day', CAST(data_anterior AS DATE), CAST(data_particao AS DATE)) > 3
ORDER BY dias_gap DESC;


-- ============================================================
-- 6. QUERIES PARA DASHBOARD
-- ============================================================

-- 6.1 Resumo diário do mercado
SELECT
    data_particao,
    COUNT(DISTINCT ticker) as qtd_acoes,
    SUM(volume_total_diario) as volume_total_mercado,
    AVG(variacao_percentual_diaria) as variacao_media_mercado
FROM b3_database.refined_b3_data
GROUP BY data_particao
ORDER BY data_particao DESC
LIMIT 30;


-- 6.2 Ranking de performance por ticker
SELECT
    ticker,
    ROUND(AVG(variacao_percentual_diaria), 4) as variacao_media_diaria,
    ROUND(SUM(variacao_percentual_diaria), 2) as variacao_acumulada,
    COUNT(*) as dias_analisados
FROM b3_database.refined_b3_data
GROUP BY ticker
ORDER BY variacao_media_diaria DESC;


-- 6.3 Correlação volume x variação (análise simplificada)
SELECT
    CASE
        WHEN volume_total_diario < 10000000 THEN 'Baixo (<10M)'
        WHEN volume_total_diario < 50000000 THEN 'Médio (10M-50M)'
        ELSE 'Alto (>50M)'
    END as faixa_volume,
    COUNT(*) as ocorrencias,
    ROUND(AVG(ABS(variacao_percentual_diaria)), 2) as variacao_abs_media
FROM b3_database.refined_b3_data
GROUP BY
    CASE
        WHEN volume_total_diario < 10000000 THEN 'Baixo (<10M)'
        WHEN volume_total_diario < 50000000 THEN 'Médio (10M-50M)'
        ELSE 'Alto (>50M)'
    END
ORDER BY variacao_abs_media DESC;


-- ============================================================
-- 7. QUERIES PARA RELATÓRIOS
-- ============================================================

-- 7.1 Relatório consolidado por ticker
SELECT
    ticker,
    COUNT(*) as total_registros,
    MIN(data_particao) as data_inicio,
    MAX(data_particao) as data_fim,
    ROUND(AVG(preco_fechamento), 2) as preco_medio,
    ROUND(MIN(preco_minimo), 2) as minima,
    ROUND(MAX(preco_maximo), 2) as maxima,
    SUM(volume_total_diario) as volume_total,
    ROUND(AVG(media_movel_7d), 2) as media_movel_media,
    ROUND(AVG(variacao_percentual_diaria), 4) as variacao_diaria_media
FROM b3_database.refined_b3_data
GROUP BY ticker
ORDER BY volume_total DESC;


-- 7.2 Exportar dados para análise externa (últimos 7 dias)
SELECT
    data_particao,
    ticker,
    preco_abertura,
    preco_maximo,
    preco_minimo,
    preco_fechamento,
    volume_total_diario,
    media_movel_7d,
    variacao_percentual_diaria
FROM b3_database.refined_b3_data
WHERE data_particao >= DATE_FORMAT(DATE_ADD('day', -7, CURRENT_DATE), '%Y-%m-%d')
ORDER BY data_particao DESC, ticker;


-- ============================================================
-- 8. CRIAR VIEWS PARA FACILITAR CONSULTAS
-- ============================================================

-- 8.1 View de resumo diário
CREATE OR REPLACE VIEW b3_database.vw_resumo_diario AS
SELECT
    data_particao,
    ticker,
    preco_abertura,
    preco_fechamento,
    preco_maximo,
    preco_minimo,
    volume_total_diario,
    media_movel_7d,
    variacao_percentual_diaria
FROM b3_database.refined_b3_data;


-- 8.2 View de performance por ticker
CREATE OR REPLACE VIEW b3_database.vw_performance_ticker AS
SELECT
    ticker,
    COUNT(*) as dias_negociados,
    ROUND(AVG(preco_fechamento), 2) as preco_medio,
    ROUND(MIN(preco_minimo), 2) as minima_periodo,
    ROUND(MAX(preco_maximo), 2) as maxima_periodo,
    SUM(volume_total_diario) as volume_total,
    ROUND(AVG(variacao_percentual_diaria), 4) as variacao_media_diaria
FROM b3_database.refined_b3_data
GROUP BY ticker;
