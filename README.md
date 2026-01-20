# Tech Challenge Fase 2 - Pipeline Batch Bovespa

Pipeline de dados completo para extrair, processar e analisar dados de ações da B3, utilizando AWS S3, Glue, Lambda e Athena.

## Requisitos do Projeto

| # | Requisito | Status |
|---|-----------|--------|
| 1 | Extração de dados de ações da B3 (granularidade diária) | Implementado |
| 2 | Ingestão no S3 em formato parquet com partição diária | Implementado |
| 3 | Trigger S3 → Lambda → Glue Job | Implementado |
| 4 | Lambda para iniciar job Glue | Implementado |
| 5 | Job Glue com transformações (agregação, renomeação, cálculo de datas) | Implementado |
| 6 | Dados refinados particionados por data e ticker | Implementado |
| 7 | Catalogação automática no Glue Catalog | Implementado |
| 8 | Dados consultáveis no Athena | Implementado |

## Arquitetura

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   API B3    │────▶│   Script    │────▶│   S3 Raw    │────▶│   Lambda    │
│  (IBOV)     │     │   Python    │     │  (parquet)  │     │  (trigger)  │
└─────────────┘     └─────────────┘     └─────────────┘     └──────┬──────┘
                                                                   │
                                                                   ▼
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Athena    │◀────│ Glue Catalog│◀────│ S3 Refined  │◀────│  Glue Job   │
│   (SQL)     │     │  (tabelas)  │     │  (parquet)  │     │   (ETL)     │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
```

## Estrutura do Projeto

```
tech-challenge-fase2-pipeline-b3/
├── scripts/
│   ├── extract_b3_real.py     # Extração de dados via API B3
│   ├── upload_to_s3.py        # Upload para S3
│   └── deploy_aws.py          # Deploy automatizado da infraestrutura
├── lambda/
│   └── trigger_glue_job.py    # Função Lambda (Python 3.11)
├── glue/
│   └── etl_job.py             # Job ETL PySpark
├── athena/
│   └── queries.sql            # Queries SQL de exemplo
├── docs/
│   └── SETUP_AWS.md           # Guia de configuração AWS
├── data/
│   └── raw/                   # Dados brutos locais
├── requirements.txt
└── README.md
```

## Como Executar

### Pré-requisitos

- Python 3.9+
- Conta AWS com permissões para S3, Lambda, Glue, Athena, IAM
- AWS CLI instalado e configurado

### Instalação Local

```bash
git clone git@github.com:milenemartins/pipeline-dados-b3.git
cd tech-challenge-fase2-pipeline-b3
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Configuração AWS e Deploy

Para o guia completo de configuração AWS, incluindo:
- Instalação do AWS CLI
- Configuração de credenciais
- Deploy automatizado da infraestrutura
- Extração e upload de dados
- Consultas no Athena
- Troubleshooting

**Consulte: [docs/SETUP_AWS.md](docs/SETUP_AWS.md)**

## Transformações do ETL (Requisito 5)

### A) Agregações Numéricas
- `volume_total_diario` - Soma do volume por ticker/dia
- `preco_medio_fechamento` - Média do preço de fechamento
- `contagem_registros` - Contagem de registros

### B) Renomeação de Colunas
- `Fechamento` → `preco_fechamento`
- `Volume` → `volume_negociado`

### C) Cálculos Baseados em Data
- `media_movel_7d` - Média móvel de 7 dias do preço
- `variacao_percentual_diaria` - Variação % em relação ao dia anterior
- `maxima_periodo` / `minima_periodo` - Valores extremos do período

## Dados

### Fonte
- API oficial da B3 para composição do IBOV
- Top 10 ações por participação no índice

### Ações Incluídas (exemplo)
| Ticker | Empresa | Participação IBOV |
|--------|---------|-------------------|
| VALE3 | Vale | 12.08% |
| ITUB4 | Itaú Unibanco | 8.29% |
| PETR4 | Petrobras PN | 5.91% |
| PETR3 | Petrobras ON | 4.19% |
| BBDC4 | Bradesco | 4.02% |

### Schema dos Dados Refinados
```
data, ticker, preco_abertura, preco_maximo, preco_minimo, preco_fechamento,
volume_total_diario, preco_medio_fechamento, contagem_registros,
media_movel_7d, variacao_percentual_diaria, maxima_periodo, minima_periodo,
data_particao, ticker_completo, dividendos, dados_validos, data_processamento
```

## Recursos AWS Criados

| Serviço | Nome | Descrição |
|---------|------|-----------|
| S3 | `tech-challenge-b3-*` | Bucket para dados raw e refined |
| Lambda | `TriggerGlueJobB3` | Trigger para iniciar Glue Job |
| Glue Job | `b3-etl-job` | ETL PySpark |
| Glue Database | `b3_database` | Catálogo de dados |
| Glue Table | `refined` | Tabela dos dados processados |

## Tecnologias Utilizadas

- **Python 3.9+** - Scripts de extração e deploy
- **AWS S3** - Armazenamento de dados
- **AWS Lambda** - Trigger serverless
- **AWS Glue** - ETL e catálogo de dados
- **AWS Athena** - Consultas SQL
- **PySpark** - Processamento de dados
- **Parquet** - Formato de armazenamento colunar

## Autor
Milene Martins
Projeto desenvolvido para o Tech Challenge Fase 2 - Pós-Tech FIAP
