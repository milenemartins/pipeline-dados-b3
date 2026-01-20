"""
Script de Upload para S3
Tech Challenge Fase 2 - Pipeline Batch Bovespa

Este script faz upload dos dados extraídos para o S3, mantendo a estrutura
de particionamento diário.
"""

import boto3
import os
import logging
from pathlib import Path
from botocore.exceptions import ClientError, NoCredentialsError
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class S3Uploader:
    """Classe para fazer upload de dados para S3"""

    def __init__(self, bucket_name, region='us-east-1'):
        """
        Inicializa o uploader

        Args:
            bucket_name: Nome do bucket S3
            region: Região AWS (default: us-east-1)
        """
        self.bucket_name = bucket_name
        self.region = region

        try:
            self.s3_client = boto3.client('s3', region_name=region)
            logger.info(f"Cliente S3 inicializado para região {region}")
        except NoCredentialsError:
            logger.error("Credenciais AWS não encontradas!")
            logger.error("Configure usando: aws configure")
            raise

    def create_bucket_if_not_exists(self):
        """Cria o bucket S3 se ele não existir"""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"Bucket '{self.bucket_name}' já existe")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                try:
                    if self.region == 'us-east-1':
                        self.s3_client.create_bucket(Bucket=self.bucket_name)
                    else:
                        self.s3_client.create_bucket(
                            Bucket=self.bucket_name,
                            CreateBucketConfiguration={'LocationConstraint': self.region}
                        )
                    logger.info(f"Bucket '{self.bucket_name}' criado com sucesso")
                except ClientError as create_error:
                    logger.error(f"Erro ao criar bucket: {create_error}")
                    raise
            else:
                logger.error(f"Erro ao verificar bucket: {e}")
                raise

    def upload_file(self, local_path, s3_key):
        """
        Faz upload de um arquivo para o S3

        Args:
            local_path: Caminho local do arquivo
            s3_key: Chave (path) do arquivo no S3

        Returns:
            True se upload foi bem-sucedido, False caso contrário
        """
        try:
            self.s3_client.upload_file(
                local_path,
                self.bucket_name,
                s3_key,
                ExtraArgs={'ServerSideEncryption': 'AES256'}
            )
            logger.info(f"Upload: {s3_key}")
            return True
        except ClientError as e:
            logger.error(f"Erro no upload de {local_path}: {e}")
            return False

    def upload_directory(self, local_dir, s3_prefix='raw'):
        """
        Faz upload de um diretório completo mantendo a estrutura

        Args:
            local_dir: Diretório local com os dados
            s3_prefix: Prefixo no S3 (default: 'raw')

        Returns:
            Tupla (sucessos, falhas)
        """
        local_path = Path(local_dir)

        if not local_path.exists():
            raise ValueError(f"Diretório não encontrado: {local_dir}")

        logger.info(f"Iniciando upload de {local_dir} para s3://{self.bucket_name}/{s3_prefix}")

        success_count = 0
        fail_count = 0
        total_size = 0

        for file_path in local_path.rglob('*.parquet'):
            relative_path = file_path.relative_to(local_path)
            s3_key = f"{s3_prefix}/{relative_path}".replace('\\', '/')

            if self.upload_file(str(file_path), s3_key):
                success_count += 1
                total_size += file_path.stat().st_size
            else:
                fail_count += 1

        total_size_mb = total_size / (1024 * 1024)

        logger.info("="*60)
        logger.info("RESUMO DO UPLOAD:")
        logger.info(f"- Arquivos enviados com sucesso: {success_count}")
        logger.info(f"- Arquivos com falha: {fail_count}")
        logger.info(f"- Tamanho total: {total_size_mb:.2f} MB")
        logger.info("="*60)

        return success_count, fail_count

    def list_files(self, prefix='raw', max_files=20):
        """
        Lista arquivos no bucket S3

        Args:
            prefix: Prefixo para filtrar (default: 'raw')
            max_files: Número máximo de arquivos para listar

        Returns:
            Lista de chaves S3
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix,
                MaxKeys=max_files
            )

            if 'Contents' not in response:
                logger.info(f"Nenhum arquivo encontrado com prefixo '{prefix}'")
                return []

            files = [obj['Key'] for obj in response['Contents']]
            logger.info(f"Encontrados {len(files)} arquivos com prefixo '{prefix}':")
            for file in files:
                logger.info(f"  - {file}")

            return files

        except ClientError as e:
            logger.error(f"Erro ao listar arquivos: {e}")
            return []

    def verify_upload(self, local_dir, s3_prefix='raw'):
        """
        Verifica se todos os arquivos locais foram enviados para o S3

        Args:
            local_dir: Diretório local
            s3_prefix: Prefixo no S3

        Returns:
            Tupla (arquivos_ok, arquivos_faltando)
        """
        local_path = Path(local_dir)
        local_files = list(local_path.rglob('*.parquet'))

        logger.info(f"Verificando {len(local_files)} arquivos locais...")

        files_ok = 0
        files_missing = []

        for file_path in local_files:
            relative_path = file_path.relative_to(local_path)
            s3_key = f"{s3_prefix}/{relative_path}".replace('\\', '/')

            try:
                self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
                files_ok += 1
            except ClientError:
                files_missing.append(s3_key)
                logger.warning(f"Arquivo não encontrado no S3: {s3_key}")

        logger.info(f"Verificação concluída: {files_ok}/{len(local_files)} arquivos OK")

        if files_missing:
            logger.warning(f"{len(files_missing)} arquivo(s) faltando no S3")

        return files_ok, files_missing

    def setup_bucket_notification(self, lambda_function_arn):
        """
        Configura notificação do bucket para acionar Lambda

        Args:
            lambda_function_arn: ARN da função Lambda

        Note:
            Requer permissões adequadas de IAM
        """
        notification_configuration = {
            'LambdaFunctionConfigurations': [
                {
                    'Id': 'TriggerGlueJobOnUpload',
                    'LambdaFunctionArn': lambda_function_arn,
                    'Events': ['s3:ObjectCreated:*'],
                    'Filter': {
                        'Key': {
                            'FilterRules': [
                                {'Name': 'prefix', 'Value': 'raw/'},
                                {'Name': 'suffix', 'Value': '.parquet'}
                            ]
                        }
                    }
                }
            ]
        }

        try:
            self.s3_client.put_bucket_notification_configuration(
                Bucket=self.bucket_name,
                NotificationConfiguration=notification_configuration
            )
            logger.info("Notificação do bucket configurada com sucesso")
        except ClientError as e:
            logger.error(f"Erro ao configurar notificação: {e}")
            logger.error("Verifique se a função Lambda tem permissão para ser invocada pelo S3")
            raise


def main():
    """Função principal"""
    import argparse

    parser = argparse.ArgumentParser(description='Upload de dados da B3 para S3')
    parser.add_argument(
        '--bucket',
        type=str,
        required=True,
        help='Nome do bucket S3'
    )
    parser.add_argument(
        '--region',
        type=str,
        default='us-east-1',
        help='Região AWS (default: us-east-1)'
    )
    parser.add_argument(
        '--local-dir',
        type=str,
        default='../data/raw',
        help='Diretório local com os dados (default: ../data/raw)'
    )
    parser.add_argument(
        '--s3-prefix',
        type=str,
        default='raw',
        help='Prefixo no S3 (default: raw)'
    )
    parser.add_argument(
        '--create-bucket',
        action='store_true',
        help='Criar bucket se não existir'
    )
    parser.add_argument(
        '--verify',
        action='store_true',
        help='Verificar upload após conclusão'
    )

    args = parser.parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    local_dir = os.path.join(script_dir, args.local_dir)
    local_dir = os.path.normpath(local_dir)

    logger.info("="*60)
    logger.info("UPLOAD DE DADOS PARA S3")
    logger.info("="*60)
    logger.info(f"Bucket: {args.bucket}")
    logger.info(f"Região: {args.region}")
    logger.info(f"Diretório local: {local_dir}")
    logger.info(f"Prefixo S3: {args.s3_prefix}")
    logger.info("="*60)

    uploader = S3Uploader(bucket_name=args.bucket, region=args.region)

    if args.create_bucket:
        uploader.create_bucket_if_not_exists()

    success, failures = uploader.upload_directory(local_dir, args.s3_prefix)

    if args.verify:
        logger.info("\nVerificando upload...")
        uploader.verify_upload(local_dir, args.s3_prefix)

    logger.info("\nArquivos no S3 (amostra):")
    uploader.list_files(prefix=args.s3_prefix, max_files=10)

    if failures > 0:
        logger.error(f"\nUpload concluído com {failures} falha(s)")
        return 1
    else:
        logger.info("\nUpload concluído com sucesso!")
        return 0


if __name__ == '__main__':
    exit(main())
