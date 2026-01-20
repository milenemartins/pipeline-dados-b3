"""
Lambda Function - Trigger Glue Job
Tech Challenge Fase 2 - Pipeline Batch Bovespa

Esta função Lambda é acionada quando arquivos parquet são criados no S3 (pasta raw/)
e inicia o Glue Job para processamento ETL.
"""

import json
import boto3
import logging
import os
from urllib.parse import unquote_plus

logger = logging.getLogger()
logger.setLevel(logging.INFO)

glue_client = boto3.client('glue')

GLUE_JOB_NAME = os.environ.get('GLUE_JOB_NAME', 'b3-etl-job')
OUTPUT_BUCKET = os.environ.get('OUTPUT_BUCKET', '')
OUTPUT_PREFIX = os.environ.get('OUTPUT_PREFIX', 'refined')
DATABASE_NAME = os.environ.get('DATABASE_NAME', 'b3_database')


def lambda_handler(event, context):
    """
    Handler principal da Lambda.

    Processa eventos S3 quando arquivos parquet são criados no bucket.
    Inicia o Glue Job passando informações do arquivo.

    Args:
        event: Evento S3 com informações do arquivo criado
        context: Contexto de execução Lambda

    Returns:
        dict: Resposta com status da execução
    """
    logger.info(f"Evento recebido: {json.dumps(event)}")

    try:
        for record in event.get('Records', []):
            event_name = record.get('eventName', '')

            if not event_name.startswith('ObjectCreated'):
                logger.info(f"Evento ignorado (não é criação): {event_name}")
                continue

            s3_info = record.get('s3', {})
            bucket_name = s3_info.get('bucket', {}).get('name', '')
            object_key = unquote_plus(s3_info.get('object', {}).get('key', ''))
            object_size = s3_info.get('object', {}).get('size', 0)

            logger.info(f"Processando arquivo: s3://{bucket_name}/{object_key}")
            logger.info(f"Tamanho: {object_size} bytes")

            if not object_key.startswith('raw/') or not object_key.endswith('.parquet'):
                logger.info(f"Arquivo ignorado (não é parquet em raw/): {object_key}")
                continue

            partition_info = extract_partition_info(object_key)

            output_bucket = OUTPUT_BUCKET if OUTPUT_BUCKET else bucket_name

            job_arguments = {
                '--input_bucket': bucket_name,
                '--input_key': object_key,
                '--output_bucket': output_bucket,
                '--output_prefix': OUTPUT_PREFIX,
                '--database_name': DATABASE_NAME,
                '--source_file': f"s3://{bucket_name}/{object_key}"
            }

            if partition_info:
                job_arguments.update({
                    '--ano': str(partition_info.get('ano', '')),
                    '--mes': str(partition_info.get('mes', '')),
                    '--dia': str(partition_info.get('dia', ''))
                })

            logger.info(f"Argumentos do Job: {json.dumps(job_arguments)}")

            response = start_glue_job(GLUE_JOB_NAME, job_arguments)

            logger.info(f"Glue Job iniciado: {response}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Processamento concluído com sucesso',
                'glue_job': GLUE_JOB_NAME
            })
        }

    except Exception as e:
        logger.error(f"Erro ao processar evento: {str(e)}")
        raise


def extract_partition_info(object_key):
    """
    Extrai informações de particionamento do path S3.

    Args:
        object_key: Chave do objeto S3

    Returns:
        dict: Dicionário com ano, mes, dia ou None se não encontrado
    """
    partition_info = {}

    try:
        parts = object_key.split('/')

        for part in parts:
            if '=' in part:
                key, value = part.split('=', 1)
                if key in ['ano', 'mes', 'dia']:
                    partition_info[key] = value

        if partition_info:
            logger.info(f"Partições extraídas: {partition_info}")
            return partition_info

    except Exception as e:
        logger.warning(f"Erro ao extrair partições: {str(e)}")

    return None


def start_glue_job(job_name, arguments):
    """
    Inicia execução do Glue Job.

    Args:
        job_name: Nome do Glue Job
        arguments: Argumentos para passar ao job

    Returns:
        dict: Resposta da API do Glue
    """
    try:
        response = glue_client.start_job_run(
            JobName=job_name,
            Arguments=arguments
        )

        job_run_id = response.get('JobRunId', '')
        logger.info(f"Glue Job '{job_name}' iniciado com RunId: {job_run_id}")

        return {
            'job_name': job_name,
            'job_run_id': job_run_id,
            'status': 'STARTED'
        }

    except glue_client.exceptions.EntityNotFoundException:
        logger.error(f"Glue Job '{job_name}' não encontrado")
        raise

    except glue_client.exceptions.ConcurrentRunsExceededException:
        logger.warning(f"Glue Job '{job_name}' já está em execução (limite de execuções concorrentes)")
        return {
            'job_name': job_name,
            'status': 'ALREADY_RUNNING'
        }

    except Exception as e:
        logger.error(f"Erro ao iniciar Glue Job: {str(e)}")
        raise
