"""
AWS Glue ETL Job - Transformações de Dados B3
Tech Challenge Fase 2 - Pipeline Batch Bovespa

Este script PySpark realiza as transformações ETL dos dados brutos da B3,
aplicando agregações, renomeações e cálculos de métricas.

Transformações implementadas:
A) Agregação numérica por ticker (volume total, preço médio, contagem)
B) Renomeação de colunas (Fechamento -> preco_fechamento, Volume -> volume_negociado)
C) Cálculos baseados em data (média móvel 7 dias, variação percentual, máx/mín)

Output: Parquet com compressão Snappy particionado por data e ticker
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, DateType, StringType


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'input_bucket',
    'input_key',
    'output_bucket',
    'output_prefix',
    'database_name'
])

job.init(args['JOB_NAME'], args)

INPUT_BUCKET = args['input_bucket']
INPUT_KEY = args['input_key']
OUTPUT_BUCKET = args['output_bucket']
OUTPUT_PREFIX = args['output_prefix']
DATABASE_NAME = args['database_name']

INPUT_PATH = f"s3://{INPUT_BUCKET}/{INPUT_KEY}"
RAW_PATH = f"s3://{INPUT_BUCKET}/raw/"
OUTPUT_PATH = f"s3://{OUTPUT_BUCKET}/{OUTPUT_PREFIX}/"

print(f"=" * 60)
print("INICIANDO ETL JOB - DADOS B3")
print(f"=" * 60)
print(f"Input Path: {INPUT_PATH}")
print(f"Raw Path: {RAW_PATH}")
print(f"Output Path: {OUTPUT_PATH}")
print(f"Database: {DATABASE_NAME}")
print(f"=" * 60)


def read_raw_data():
    """
    Lê dados brutos do S3.
    Tenta ler o arquivo específico ou toda a pasta raw/ se não encontrar.
    """
    try:
        df = spark.read.parquet(INPUT_PATH)
        print(f"Lidos {df.count()} registros do arquivo específico")
    except Exception as e:
        print(f"Erro ao ler arquivo específico: {e}")
        print("Tentando ler toda a pasta raw/...")
        df = spark.read.parquet(RAW_PATH)
        print(f"Lidos {df.count()} registros da pasta raw/")

    return df


def rename_columns(df):
    """
    Renomeia colunas conforme especificado:
    - Fechamento -> preco_fechamento
    - Volume -> volume_negociado
    """
    print("\n--- Renomeando colunas ---")

    column_mapping = {
        'Fechamento': 'preco_fechamento',
        'Volume': 'volume_negociado',
        'Abertura': 'preco_abertura',
        'Maxima': 'preco_maximo',
        'Minima': 'preco_minimo',
        'Data': 'data',
        'Ticker': 'ticker',
        'TickerCompleto': 'ticker_completo',
        'Dividendos': 'dividendos',
        'Desdobramentos': 'desdobramentos',
        'Ano': 'ano',
        'Mes': 'mes',
        'Dia': 'dia'
    }

    for old_name, new_name in column_mapping.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)
            print(f"  {old_name} -> {new_name}")

    return df


def calculate_aggregations(df):
    """
    Calcula agregações numéricas por ticker:
    - Volume total diário
    - Preço médio de fechamento
    - Contagem de registros
    """
    print("\n--- Calculando agregações por ticker ---")

    agg_df = df.groupBy('ticker', 'data').agg(
        F.sum('volume_negociado').alias('volume_total_diario'),
        F.avg('preco_fechamento').alias('preco_medio_fechamento'),
        F.count('*').alias('contagem_registros'),
        F.first('preco_abertura').alias('preco_abertura'),
        F.max('preco_maximo').alias('preco_maximo'),
        F.min('preco_minimo').alias('preco_minimo'),
        F.last('preco_fechamento').alias('preco_fechamento'),
        F.sum('dividendos').alias('dividendos'),
        F.first('ticker_completo').alias('ticker_completo')
    )

    print(f"  Agregações calculadas para {agg_df.count()} combinações ticker/data")

    return agg_df


def calculate_date_based_metrics(df):
    """
    Calcula métricas baseadas em data:
    - Média móvel de 7 dias do preço de fechamento
    - Variação percentual diária
    - Máxima e mínima do período
    """
    print("\n--- Calculando métricas baseadas em data ---")

    window_7d = Window.partitionBy('ticker').orderBy('data').rowsBetween(-6, 0)
    window_ticker = Window.partitionBy('ticker').orderBy('data')
    window_all = Window.partitionBy('ticker')

    df = df.withColumn(
        'media_movel_7d',
        F.avg('preco_fechamento').over(window_7d)
    )
    print("  Calculada: média móvel 7 dias")

    df = df.withColumn(
        'preco_fechamento_anterior',
        F.lag('preco_fechamento', 1).over(window_ticker)
    )

    df = df.withColumn(
        'variacao_percentual_diaria',
        F.when(
            F.col('preco_fechamento_anterior').isNotNull(),
            ((F.col('preco_fechamento') - F.col('preco_fechamento_anterior')) /
             F.col('preco_fechamento_anterior') * 100)
        ).otherwise(0.0)
    )
    print("  Calculada: variação percentual diária")

    df = df.withColumn(
        'maxima_periodo',
        F.max('preco_maximo').over(window_all)
    )

    df = df.withColumn(
        'minima_periodo',
        F.min('preco_minimo').over(window_all)
    )
    print("  Calculadas: máxima e mínima do período")

    df = df.drop('preco_fechamento_anterior')

    return df


def add_partition_columns(df):
    """
    Adiciona colunas de particionamento no formato desejado.
    """
    print("\n--- Adicionando colunas de particionamento ---")

    df = df.withColumn('data_particao', F.date_format('data', 'yyyy-MM-dd'))

    df = df.withColumn('ano', F.year('data'))
    df = df.withColumn('mes', F.month('data'))
    df = df.withColumn('dia', F.dayofmonth('data'))

    return df


def add_quality_columns(df):
    """
    Adiciona colunas de qualidade e metadados.
    """
    print("\n--- Adicionando colunas de qualidade ---")

    df = df.withColumn('data_processamento', F.current_timestamp())

    df = df.withColumn(
        'dados_validos',
        F.when(
            (F.col('preco_fechamento').isNotNull()) &
            (F.col('volume_total_diario').isNotNull()) &
            (F.col('preco_fechamento') > 0),
            True
        ).otherwise(False)
    )

    return df


def write_to_refined(df):
    """
    Escreve dados transformados para a camada refined no S3.
    Formato: Parquet com compressão Snappy
    Particionamento: data=YYYY-MM-DD/ticker=XXXX/
    """
    print("\n--- Escrevendo dados refinados ---")
    print(f"  Destino: {OUTPUT_PATH}")

    df.write \
        .mode('overwrite') \
        .partitionBy('data_particao', 'ticker') \
        .option('compression', 'snappy') \
        .parquet(OUTPUT_PATH)

    print("  Dados escritos com sucesso!")


def register_in_catalog(df):
    """
    Registra a tabela no Glue Catalog para consulta via Athena.
    """
    print("\n--- Registrando no Glue Catalog ---")

    try:
        dynamic_frame = DynamicFrame.fromDF(df, glueContext, "refined_data")

        sink = glueContext.getSink(
            connection_type="s3",
            path=OUTPUT_PATH,
            enableUpdateCatalog=True,
            updateBehavior="UPDATE_IN_DATABASE",
            partitionKeys=["data_particao", "ticker"]
        )
        sink.setFormat("parquet", compression="snappy")
        sink.setCatalogInfo(catalogDatabase=DATABASE_NAME, catalogTableName="refined_b3_data")
        sink.writeFrame(dynamic_frame)

        print(f"  Tabela 'refined_b3_data' registrada no database '{DATABASE_NAME}'")

    except Exception as e:
        print(f"  Aviso: Não foi possível registrar no catálogo: {e}")
        print("  Os dados foram salvos no S3 e podem ser catalogados manualmente.")


def create_summary_statistics(df):
    """
    Cria estatísticas resumidas para validação.
    """
    print("\n--- Estatísticas do processamento ---")

    ticker_counts = df.groupBy('ticker').count().collect()
    print("\n  Registros por ticker:")
    for row in ticker_counts:
        print(f"    {row['ticker']}: {row['count']}")

    date_range = df.agg(
        F.min('data').alias('data_inicio'),
        F.max('data').alias('data_fim')
    ).collect()[0]
    print(f"\n  Período: {date_range['data_inicio']} até {date_range['data_fim']}")

    total = df.count()
    print(f"\n  Total de registros processados: {total}")

    return total


def main():
    """
    Função principal do ETL.
    """
    try:
        print("\n[1/7] Lendo dados brutos...")
        raw_df = read_raw_data()
        print(f"  Schema original:")
        raw_df.printSchema()

        print("\n[2/7] Renomeando colunas...")
        renamed_df = rename_columns(raw_df)

        print("\n[3/7] Calculando agregações...")
        agg_df = calculate_aggregations(renamed_df)

        print("\n[4/7] Calculando métricas temporais...")
        metrics_df = calculate_date_based_metrics(agg_df)

        print("\n[5/7] Preparando particionamento...")
        partitioned_df = add_partition_columns(metrics_df)

        print("\n[6/7] Adicionando metadados de qualidade...")
        final_df = add_quality_columns(partitioned_df)

        print("\n  Schema final:")
        final_df.printSchema()

        print("\n[7/7] Escrevendo dados refinados...")
        write_to_refined(final_df)

        register_in_catalog(final_df)

        total_records = create_summary_statistics(final_df)

        print("\n" + "=" * 60)
        print("ETL CONCLUÍDO COM SUCESSO!")
        print(f"Total de registros processados: {total_records}")
        print(f"Dados disponíveis em: {OUTPUT_PATH}")
        print("=" * 60)

    except Exception as e:
        print(f"\n[ERRO] Falha no ETL: {str(e)}")
        raise

    finally:
        job.commit()


if __name__ == "__main__":
    main()
