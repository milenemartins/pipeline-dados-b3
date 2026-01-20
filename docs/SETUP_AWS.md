# Guia de Configuração AWS

Tech Challenge Fase 2 - Pipeline Batch Bovespa

## Pré-requisitos

1. Conta AWS ativa
2. AWS CLI instalado
3. Python 3.9+
4. Permissões IAM para: S3, Lambda, Glue, Athena, IAM

## Passo 1: Instalar AWS CLI

### macOS
```bash
brew install awscli
```

### Linux
```bash
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
```

### Verificar instalação
```bash
aws --version
```

## Passo 2: Configurar Credenciais

### Criar Access Key no Console AWS

1. Acesse https://console.aws.amazon.com
2. Clique no seu nome (canto superior direito) → **Security credentials**
3. Em **Access keys**, clique em **Create access key**
4. Selecione **Command Line Interface (CLI)**
5. Copie o **Access Key ID** e **Secret Access Key**

### Configurar AWS CLI

```bash
aws configure
```

Informe:
- AWS Access Key ID: `AKIA...`
- AWS Secret Access Key: `sua-chave-secreta`
- Default region name: `us-east-1`
- Default output format: `json`

### Verificar configuração

```bash
aws sts get-caller-identity
```

## Passo 3: Deploy Automatizado

O script `deploy_aws.py` cria toda a infraestrutura automaticamente:

```bash
cd tech-challenge-fase2-pipeline-b3
python scripts/deploy_aws.py --bucket SEU-BUCKET-UNICO --region us-east-1
```

### O que é criado automaticamente:

| Recurso | Nome | Descrição |
|---------|------|-----------|
| S3 Bucket | `SEU-BUCKET-UNICO` | Armazenamento de dados |
| S3 Folders | `raw/`, `refined/`, `scripts/` | Estrutura de pastas |
| IAM Role | `TechChallengeB3LambdaRole` | Permissões da Lambda |
| IAM Role | `TechChallengeB3GlueRole` | Permissões do Glue |
| Lambda | `TriggerGlueJobB3` | Função trigger |
| Glue Database | `b3_database` | Catálogo de dados |
| Glue Job | `b3-etl-job` | ETL PySpark |
| S3 Trigger | `raw/*.parquet → Lambda` | Notificação automática |

## Passo 4: Extrair e Carregar Dados

### Extrair dados da B3

```bash
python scripts/extract_b3_real.py --days 30 --top 10
```

Parâmetros:
- `--days`: Número de dias de histórico (default: 30)
- `--top`: Número de ações do IBOV (default: 15)

### Upload para S3

```bash
python scripts/upload_to_s3.py --bucket SEU-BUCKET --local-dir ../data/raw --verify
```

O upload aciona automaticamente:
1. **S3 Event** → detecta novo arquivo `.parquet` em `raw/`
2. **Lambda** → inicia o Glue Job
3. **Glue Job** → processa dados e salva em `refined/`
4. **Glue Catalog** → catalogação via Crawler

## Passo 5: Verificar Execução

### Verificar Glue Job

```bash
aws glue get-job-runs --job-name b3-etl-job --max-items 3
```

Status esperado: `SUCCEEDED`

### Verificar dados refinados

```bash
aws s3 ls s3://SEU-BUCKET/refined/ --recursive | head -10
```

### Verificar tabela no catálogo

```bash
aws glue get-tables --database-name b3_database
```

## Passo 6: Consultar no Athena

### Via Console AWS

1. Acesse **AWS Athena** no console
2. Selecione Database: `b3_database`
3. Execute queries SQL

### Via AWS CLI

```bash
# Executar query
aws athena start-query-execution \
  --query-string "SELECT * FROM b3_database.refined LIMIT 10" \
  --query-execution-context Database=b3_database \
  --result-configuration OutputLocation=s3://SEU-BUCKET/athena-results/

# Verificar resultado (usar o QueryExecutionId retornado)
aws athena get-query-results --query-execution-id <ID>
```

### Queries de Exemplo

```sql
-- Resumo por ticker
SELECT
    ticker,
    COUNT(*) as dias,
    ROUND(AVG(preco_fechamento), 2) as preco_medio,
    ROUND(SUM(volume_total_diario)/1000000, 1) as volume_milhoes
FROM b3_database.refined
GROUP BY ticker
ORDER BY volume_milhoes DESC;

-- Evolução com média móvel
SELECT
    data_particao,
    ticker,
    preco_fechamento,
    media_movel_7d,
    variacao_percentual_diaria
FROM b3_database.refined
WHERE ticker = 'VALE3'
ORDER BY data_particao DESC
LIMIT 10;
```

## Custos Estimados

| Serviço | Uso | Custo Estimado |
|---------|-----|----------------|
| S3 | ~1 MB | < $0.01/mês |
| Lambda | ~100 invocações | Free Tier |
| Glue | ~2 DPU-hour | ~$0.88 |
| Athena | ~10 MB scanned | < $0.01 |

**Total estimado:** < $1.00 para teste do pipeline

## Limpeza de Recursos

### Via script (recomendado)

```bash
python scripts/deploy_aws.py --bucket SEU-BUCKET --cleanup
```

### Manualmente

```bash
# Deletar bucket S3 (com todos os dados)
aws s3 rb s3://SEU-BUCKET --force

# Deletar Lambda
aws lambda delete-function --function-name TriggerGlueJobB3

# Deletar Glue Job
aws glue delete-job --job-name b3-etl-job

# Deletar Glue Database
aws glue delete-database --name b3_database

# Deletar Crawler (se criado)
aws glue delete-crawler --name b3-refined-crawler
```

## Troubleshooting

### Erro de permissão no S3
```bash
aws s3api get-bucket-policy --bucket SEU-BUCKET
```

### Erro na Lambda
```bash
aws logs tail /aws/lambda/TriggerGlueJobB3 --follow
```

### Erro no Glue Job
```bash
aws glue get-job-runs --job-name b3-etl-job --query 'JobRuns[0].ErrorMessage'
```

### Erro de timestamp no Glue
Se aparecer erro `Illegal Parquet type: INT64 (TIMESTAMP(NANOS,false))`:
- O script `extract_b3_real.py` já converte timestamps para formato compatível
- Caso persista, verifique se os dados foram gerados com a versão mais recente do script

## Arquitetura Detalhada

```
┌──────────────────────────────────────────────────────────────────┐
│                         AWS Cloud                                 │
├──────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │                        S3 Bucket                            │ │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │ │
│  │  │  raw/    │  │ refined/ │  │ scripts/ │  │ athena-  │   │ │
│  │  │ parquet  │  │ parquet  │  │ etl_job  │  │ results/ │   │ │
│  │  └────┬─────┘  └────▲─────┘  └──────────┘  └──────────┘   │ │
│  └───────┼─────────────┼───────────────────────────────────────┘ │
│          │             │                                         │
│          ▼             │                                         │
│  ┌──────────────┐      │      ┌──────────────────────────────┐  │
│  │    Lambda    │──────┼─────▶│         Glue Job             │  │
│  │ TriggerGlue  │      │      │  - Agregações                │  │
│  │  JobB3       │      │      │  - Renomeações               │  │
│  └──────────────┘      │      │  - Cálculos temporais        │  │
│                        │      └──────────────┬───────────────┘  │
│                        │                     │                   │
│                        │                     ▼                   │
│                        │      ┌──────────────────────────────┐  │
│                        └──────│       Glue Catalog           │  │
│                               │  - b3_database               │  │
│                               │  - tabela: refined           │  │
│                               └──────────────┬───────────────┘  │
│                                              │                   │
│                                              ▼                   │
│                               ┌──────────────────────────────┐  │
│                               │          Athena              │  │
│                               │  SELECT * FROM refined       │  │
│                               └──────────────────────────────┘  │
│                                                                   │
└──────────────────────────────────────────────────────────────────┘
```

## Referências

- [AWS Glue Documentation](https://docs.aws.amazon.com/glue/)
- [AWS Lambda Documentation](https://docs.aws.amazon.com/lambda/)
- [AWS Athena Documentation](https://docs.aws.amazon.com/athena/)
- [B3 - Composição do IBOV](https://www.b3.com.br/pt_br/market-data-e-indices/)
