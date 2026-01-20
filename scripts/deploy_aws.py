"""
Script de Deploy AWS
Tech Challenge Fase 2 - Pipeline Batch Bovespa

Este script automatiza o deploy de toda a infraestrutura AWS necessária:
- Criação do bucket S3
- Deploy da função Lambda
- Criação do Glue Job
- Configuração do trigger S3 → Lambda
- Criação do database no Glue Catalog
"""

import boto3
import json
import os
import sys
import time
import zipfile
import logging
import argparse
from pathlib import Path
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AWSDeployer:
    """Classe para deploy automatizado dos recursos AWS"""

    def __init__(self, config):
        """
        Inicializa o deployer com configurações.

        Args:
            config: Dicionário com configurações do deploy
        """
        self.config = config
        self.region = config.get('region', 'us-east-1')
        self.account_id = None

        self.s3 = boto3.client('s3', region_name=self.region)
        self.lambda_client = boto3.client('lambda', region_name=self.region)
        self.glue = boto3.client('glue', region_name=self.region)
        self.iam = boto3.client('iam', region_name=self.region)
        self.sts = boto3.client('sts', region_name=self.region)

        self._get_account_id()

    def _get_account_id(self):
        """Obtém o ID da conta AWS"""
        try:
            response = self.sts.get_caller_identity()
            self.account_id = response['Account']
            logger.info(f"Account ID: {self.account_id}")
        except Exception as e:
            logger.error(f"Erro ao obter Account ID: {e}")
            raise

    def deploy_all(self):
        """Executa o deploy completo de todos os recursos"""
        logger.info("=" * 60)
        logger.info("INICIANDO DEPLOY COMPLETO")
        logger.info("=" * 60)

        try:
            self.create_s3_bucket()

            lambda_role_arn = self.create_lambda_role()
            glue_role_arn = self.create_glue_role()

            logger.info("Aguardando propagação das IAM Roles...")
            time.sleep(10)

            self.create_glue_database()

            self.upload_glue_script()

            self.create_glue_job(glue_role_arn)

            lambda_arn = self.deploy_lambda(lambda_role_arn)

            self.configure_s3_trigger(lambda_arn)

            logger.info("\n" + "=" * 60)
            logger.info("DEPLOY CONCLUÍDO COM SUCESSO!")
            logger.info("=" * 60)
            self._print_summary()

        except Exception as e:
            logger.error(f"Erro no deploy: {e}")
            raise

    def create_s3_bucket(self):
        """Cria o bucket S3 se não existir"""
        bucket_name = self.config['bucket_name']
        logger.info(f"\n[1/7] Criando bucket S3: {bucket_name}")

        try:
            self.s3.head_bucket(Bucket=bucket_name)
            logger.info(f"  Bucket '{bucket_name}' já existe")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                try:
                    if self.region == 'us-east-1':
                        self.s3.create_bucket(Bucket=bucket_name)
                    else:
                        self.s3.create_bucket(
                            Bucket=bucket_name,
                            CreateBucketConfiguration={'LocationConstraint': self.region}
                        )
                    logger.info(f"  Bucket '{bucket_name}' criado")

                    self.s3.put_bucket_encryption(
                        Bucket=bucket_name,
                        ServerSideEncryptionConfiguration={
                            'Rules': [{
                                'ApplyServerSideEncryptionByDefault': {
                                    'SSEAlgorithm': 'AES256'
                                }
                            }]
                        }
                    )
                    logger.info("  Criptografia habilitada")

                except ClientError as create_error:
                    logger.error(f"  Erro ao criar bucket: {create_error}")
                    raise
            else:
                raise

        for prefix in ['raw/', 'refined/', 'scripts/']:
            self.s3.put_object(Bucket=bucket_name, Key=prefix)
        logger.info("  Estrutura de pastas criada (raw/, refined/, scripts/)")

    def create_lambda_role(self):
        """Cria IAM Role para a Lambda"""
        role_name = self.config.get('lambda_role_name', 'TechChallengeB3LambdaRole')
        logger.info(f"\n[2/7] Criando IAM Role para Lambda: {role_name}")

        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }]
        }

        permissions_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "logs:CreateLogGroup",
                        "logs:CreateLogStream",
                        "logs:PutLogEvents"
                    ],
                    "Resource": "arn:aws:logs:*:*:*"
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetObject",
                        "s3:ListBucket"
                    ],
                    "Resource": [
                        f"arn:aws:s3:::{self.config['bucket_name']}",
                        f"arn:aws:s3:::{self.config['bucket_name']}/*"
                    ]
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "glue:StartJobRun",
                        "glue:GetJobRun",
                        "glue:GetJobRuns"
                    ],
                    "Resource": "*"
                }
            ]
        }

        try:
            self.iam.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(trust_policy),
                Description='Role para Lambda do Tech Challenge B3'
            )
            logger.info(f"  Role '{role_name}' criada")
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityAlreadyExists':
                logger.info(f"  Role '{role_name}' já existe")
            else:
                raise

        policy_name = 'TechChallengeB3LambdaPolicy'
        try:
            self.iam.put_role_policy(
                RoleName=role_name,
                PolicyName=policy_name,
                PolicyDocument=json.dumps(permissions_policy)
            )
            logger.info(f"  Política '{policy_name}' anexada")
        except ClientError as e:
            logger.warning(f"  Aviso ao anexar política: {e}")

        return f"arn:aws:iam::{self.account_id}:role/{role_name}"

    def create_glue_role(self):
        """Cria IAM Role para o Glue Job"""
        role_name = self.config.get('glue_role_name', 'TechChallengeB3GlueRole')
        logger.info(f"\n[3/7] Criando IAM Role para Glue: {role_name}")

        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": "glue.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }]
        }

        permissions_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:DeleteObject",
                        "s3:ListBucket"
                    ],
                    "Resource": [
                        f"arn:aws:s3:::{self.config['bucket_name']}",
                        f"arn:aws:s3:::{self.config['bucket_name']}/*"
                    ]
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "glue:*"
                    ],
                    "Resource": "*"
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "logs:CreateLogGroup",
                        "logs:CreateLogStream",
                        "logs:PutLogEvents"
                    ],
                    "Resource": "arn:aws:logs:*:*:*"
                }
            ]
        }

        try:
            self.iam.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(trust_policy),
                Description='Role para Glue Job do Tech Challenge B3'
            )
            logger.info(f"  Role '{role_name}' criada")
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityAlreadyExists':
                logger.info(f"  Role '{role_name}' já existe")
            else:
                raise

        try:
            self.iam.attach_role_policy(
                RoleName=role_name,
                PolicyArn='arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole'
            )
            logger.info("  Política AWSGlueServiceRole anexada")
        except ClientError:
            pass

        policy_name = 'TechChallengeB3GluePolicy'
        try:
            self.iam.put_role_policy(
                RoleName=role_name,
                PolicyName=policy_name,
                PolicyDocument=json.dumps(permissions_policy)
            )
            logger.info(f"  Política '{policy_name}' anexada")
        except ClientError as e:
            logger.warning(f"  Aviso ao anexar política: {e}")

        return f"arn:aws:iam::{self.account_id}:role/{role_name}"

    def create_glue_database(self):
        """Cria o database no Glue Catalog"""
        db_name = self.config.get('database_name', 'b3_database')
        logger.info(f"\n[4/7] Criando Glue Database: {db_name}")

        try:
            self.glue.create_database(
                DatabaseInput={
                    'Name': db_name,
                    'Description': 'Database para dados da B3 - Tech Challenge Fase 2'
                }
            )
            logger.info(f"  Database '{db_name}' criado")
        except ClientError as e:
            if e.response['Error']['Code'] == 'AlreadyExistsException':
                logger.info(f"  Database '{db_name}' já existe")
            else:
                raise

    def upload_glue_script(self):
        """Faz upload do script Glue para S3"""
        logger.info("\n[5/7] Fazendo upload do script Glue para S3")

        script_dir = Path(__file__).parent.parent
        glue_script = script_dir / 'glue' / 'etl_job.py'

        if not glue_script.exists():
            raise FileNotFoundError(f"Script Glue não encontrado: {glue_script}")

        bucket_name = self.config['bucket_name']
        s3_key = 'scripts/etl_job.py'

        self.s3.upload_file(
            str(glue_script),
            bucket_name,
            s3_key
        )
        logger.info(f"  Script enviado para s3://{bucket_name}/{s3_key}")

        return f"s3://{bucket_name}/{s3_key}"

    def create_glue_job(self, role_arn):
        """Cria o Glue Job"""
        job_name = self.config.get('glue_job_name', 'b3-etl-job')
        bucket_name = self.config['bucket_name']
        script_location = f"s3://{bucket_name}/scripts/etl_job.py"

        logger.info(f"\n[6/7] Criando Glue Job: {job_name}")

        job_config = {
            'Name': job_name,
            'Description': 'ETL Job para processamento de dados da B3 - Tech Challenge Fase 2',
            'Role': role_arn,
            'Command': {
                'Name': 'glueetl',
                'ScriptLocation': script_location,
                'PythonVersion': '3'
            },
            'DefaultArguments': {
                '--enable-metrics': 'true',
                '--enable-continuous-cloudwatch-log': 'true',
                '--enable-spark-ui': 'true',
                '--spark-event-logs-path': f's3://{bucket_name}/spark-logs/',
                '--job-language': 'python',
                '--TempDir': f's3://{bucket_name}/temp/',
                '--input_bucket': bucket_name,
                '--input_key': 'raw/',
                '--output_bucket': bucket_name,
                '--output_prefix': 'refined',
                '--database_name': self.config.get('database_name', 'b3_database')
            },
            'MaxRetries': 1,
            'Timeout': 60,
            'GlueVersion': '4.0',
            'NumberOfWorkers': 2,
            'WorkerType': 'G.1X'
        }

        try:
            self.glue.create_job(**job_config)
            logger.info(f"  Job '{job_name}' criado")
        except ClientError as e:
            if e.response['Error']['Code'] == 'AlreadyExistsException':
                del job_config['Name']
                self.glue.update_job(JobName=job_name, JobUpdate=job_config)
                logger.info(f"  Job '{job_name}' atualizado")
            else:
                raise

    def deploy_lambda(self, role_arn):
        """Deploy da função Lambda"""
        function_name = self.config.get('lambda_function_name', 'TriggerGlueJobB3')
        logger.info(f"\n[7/7] Fazendo deploy da Lambda: {function_name}")

        script_dir = Path(__file__).parent.parent
        lambda_script = script_dir / 'lambda' / 'trigger_glue_job.py'

        if not lambda_script.exists():
            raise FileNotFoundError(f"Script Lambda não encontrado: {lambda_script}")

        zip_path = script_dir / 'lambda' / 'function.zip'
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.write(lambda_script, 'trigger_glue_job.py')

        logger.info(f"  Arquivo ZIP criado: {zip_path}")

        with open(zip_path, 'rb') as f:
            zip_content = f.read()

        env_vars = {
            'GLUE_JOB_NAME': self.config.get('glue_job_name', 'b3-etl-job'),
            'OUTPUT_BUCKET': self.config['bucket_name'],
            'OUTPUT_PREFIX': 'refined',
            'DATABASE_NAME': self.config.get('database_name', 'b3_database')
        }

        try:
            response = self.lambda_client.create_function(
                FunctionName=function_name,
                Runtime='python3.11',
                Role=role_arn,
                Handler='trigger_glue_job.lambda_handler',
                Code={'ZipFile': zip_content},
                Description='Trigger Glue Job quando arquivos parquet são criados no S3',
                Timeout=60,
                MemorySize=128,
                Environment={'Variables': env_vars}
            )
            lambda_arn = response['FunctionArn']
            logger.info(f"  Função '{function_name}' criada")
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceConflictException':
                self.lambda_client.update_function_code(
                    FunctionName=function_name,
                    ZipFile=zip_content
                )
                self.lambda_client.update_function_configuration(
                    FunctionName=function_name,
                    Role=role_arn,
                    Handler='trigger_glue_job.lambda_handler',
                    Timeout=60,
                    MemorySize=128,
                    Environment={'Variables': env_vars}
                )
                response = self.lambda_client.get_function(FunctionName=function_name)
                lambda_arn = response['Configuration']['FunctionArn']
                logger.info(f"  Função '{function_name}' atualizada")
            else:
                raise

        os.remove(zip_path)

        return lambda_arn

    def configure_s3_trigger(self, lambda_arn):
        """Configura o trigger S3 → Lambda"""
        bucket_name = self.config['bucket_name']
        function_name = self.config.get('lambda_function_name', 'TriggerGlueJobB3')

        logger.info("\n[8/8] Configurando trigger S3 → Lambda")

        try:
            self.lambda_client.add_permission(
                FunctionName=function_name,
                StatementId='s3-trigger-permission',
                Action='lambda:InvokeFunction',
                Principal='s3.amazonaws.com',
                SourceArn=f'arn:aws:s3:::{bucket_name}'
            )
            logger.info("  Permissão S3 → Lambda adicionada")
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceConflictException':
                logger.info("  Permissão S3 → Lambda já existe")
            else:
                raise

        notification_config = {
            'LambdaFunctionConfigurations': [{
                'Id': 'TriggerGlueJobOnParquet',
                'LambdaFunctionArn': lambda_arn,
                'Events': ['s3:ObjectCreated:*'],
                'Filter': {
                    'Key': {
                        'FilterRules': [
                            {'Name': 'prefix', 'Value': 'raw/'},
                            {'Name': 'suffix', 'Value': '.parquet'}
                        ]
                    }
                }
            }]
        }

        self.s3.put_bucket_notification_configuration(
            Bucket=bucket_name,
            NotificationConfiguration=notification_config
        )
        logger.info(f"  Trigger configurado: s3://{bucket_name}/raw/*.parquet → Lambda")

    def _print_summary(self):
        """Imprime resumo do deploy"""
        print("\n" + "=" * 60)
        print("RECURSOS CRIADOS:")
        print("=" * 60)
        print(f"Bucket S3:        {self.config['bucket_name']}")
        print(f"Lambda Function:  {self.config.get('lambda_function_name', 'TriggerGlueJobB3')}")
        print(f"Glue Job:         {self.config.get('glue_job_name', 'b3-etl-job')}")
        print(f"Glue Database:    {self.config.get('database_name', 'b3_database')}")
        print(f"Region:           {self.region}")
        print("=" * 60)
        print("\nPRÓXIMOS PASSOS:")
        print("1. Execute o upload de dados:")
        print(f"   python scripts/upload_to_s3.py --bucket {self.config['bucket_name']}")
        print("\n2. Verifique os logs da Lambda no CloudWatch")
        print("\n3. Consulte os dados refinados no Athena:")
        print(f"   SELECT * FROM {self.config.get('database_name', 'b3_database')}.refined_b3_data LIMIT 10")
        print("=" * 60)


def cleanup_resources(config, region='us-east-1'):
    """Remove todos os recursos criados"""
    logger.info("=" * 60)
    logger.info("REMOVENDO RECURSOS AWS")
    logger.info("=" * 60)

    s3 = boto3.client('s3', region_name=region)
    lambda_client = boto3.client('lambda', region_name=region)
    glue = boto3.client('glue', region_name=region)
    iam = boto3.client('iam', region_name=region)

    try:
        lambda_client.delete_function(FunctionName=config.get('lambda_function_name', 'TriggerGlueJobB3'))
        logger.info("Lambda removida")
    except ClientError as e:
        logger.warning(f"Aviso ao remover Lambda: {e}")

    try:
        glue.delete_job(JobName=config.get('glue_job_name', 'b3-etl-job'))
        logger.info("Glue Job removido")
    except ClientError as e:
        logger.warning(f"Aviso ao remover Glue Job: {e}")

    try:
        tables = glue.get_tables(DatabaseName=config.get('database_name', 'b3_database'))
        for table in tables.get('TableList', []):
            glue.delete_table(
                DatabaseName=config.get('database_name', 'b3_database'),
                Name=table['Name']
            )
        glue.delete_database(Name=config.get('database_name', 'b3_database'))
        logger.info("Glue Database removido")
    except ClientError as e:
        logger.warning(f"Aviso ao remover Glue Database: {e}")

    for role_name in [config.get('lambda_role_name', 'TechChallengeB3LambdaRole'),
                      config.get('glue_role_name', 'TechChallengeB3GlueRole')]:
        try:
            policies = iam.list_role_policies(RoleName=role_name)
            for policy_name in policies.get('PolicyNames', []):
                iam.delete_role_policy(RoleName=role_name, PolicyName=policy_name)

            attached = iam.list_attached_role_policies(RoleName=role_name)
            for policy in attached.get('AttachedPolicies', []):
                iam.detach_role_policy(RoleName=role_name, PolicyArn=policy['PolicyArn'])

            iam.delete_role(RoleName=role_name)
            logger.info(f"IAM Role '{role_name}' removida")
        except ClientError as e:
            logger.warning(f"Aviso ao remover Role {role_name}: {e}")

    logger.info("\nPara remover o bucket S3 (com todos os dados):")
    logger.info(f"  aws s3 rb s3://{config['bucket_name']} --force")


def main():
    """Função principal"""
    parser = argparse.ArgumentParser(
        description='Deploy de recursos AWS para o Tech Challenge B3'
    )
    parser.add_argument(
        '--bucket',
        type=str,
        required=True,
        help='Nome do bucket S3 (deve ser único globalmente)'
    )
    parser.add_argument(
        '--region',
        type=str,
        default='us-east-1',
        help='Região AWS (default: us-east-1)'
    )
    parser.add_argument(
        '--database',
        type=str,
        default='b3_database',
        help='Nome do database Glue (default: b3_database)'
    )
    parser.add_argument(
        '--glue-job',
        type=str,
        default='b3-etl-job',
        help='Nome do Glue Job (default: b3-etl-job)'
    )
    parser.add_argument(
        '--lambda-function',
        type=str,
        default='TriggerGlueJobB3',
        help='Nome da função Lambda (default: TriggerGlueJobB3)'
    )
    parser.add_argument(
        '--cleanup',
        action='store_true',
        help='Remove todos os recursos criados (exceto S3)'
    )

    args = parser.parse_args()

    config = {
        'bucket_name': args.bucket,
        'region': args.region,
        'database_name': args.database,
        'glue_job_name': args.glue_job,
        'lambda_function_name': args.lambda_function,
        'lambda_role_name': 'TechChallengeB3LambdaRole',
        'glue_role_name': 'TechChallengeB3GlueRole'
    }

    if args.cleanup:
        cleanup_resources(config, args.region)
    else:
        deployer = AWSDeployer(config)
        deployer.deploy_all()


if __name__ == '__main__':
    main()
