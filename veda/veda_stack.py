import os
from aws_cdk import (
    Stack,
    aws_iam as iam,
    Environment as cloud_env,
    aws_s3 as s3,
    aws_ec2 as ec2,
    aws_rds as rds,
    aws_ecs as ecs,
    aws_logs as logs,
    aws_servicediscovery as cloudmap,
    aws_secretsmanager as sm,
    aws_elasticache as elasticache,
    aws_dynamodb as dynamodb,
    SecretValue,
    RemovalPolicy as remove,
    Duration
)
from aws_cdk.aws_ecr_assets import DockerImageAsset
from constructs import Construct
from aws_cdk import CfnOutput
from cryptography.fernet import Fernet;

from dotenv import load_dotenv

import json

with open('config.json', 'r') as f:
    config = json.load(f)

load_dotenv()

class VedaStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        #configs
        db_config = config["db"]
        redis_config = config["redis"]
        cloudmap_config = config["cloudmap"]
        airflow_config = config["airflow"]
        ecommerce_config = config["ecommerce"]
        other_config = config["other"]

        #Passwords (the bad way) TODO:Remove plaintext, handle Fernet Better
        #https://docs.aws.amazon.com/cdk/api/v2/docs/aws-cdk-lib.aws_secretsmanager-readme.html gives many reasons why not to do this.
        db_password_secret = sm.Secret(self, "AirflowDBPassword",
            removal_policy=remove.DESTROY,
            secret_name=db_config["passwordSecretName"],
            secret_string_value=SecretValue.unsafe_plain_text(os.getenv("DB_PASSWORD"))
        )

        redis_password_secret = sm.Secret(self, "RedisPassword",
            removal_policy=remove.DESTROY,
            secret_name=redis_config["passwordSecretName"],
            secret_string_value=SecretValue.unsafe_plain_text(os.getenv("REDIS_PASSWORD"))
        )

        db_password_secret = sm.Secret(self, "EcomDBPassword",
            removal_policy=remove.DESTROY,
            secret_name=ecommerce_config["passwordSecretName"],
            secret_string_value=SecretValue.unsafe_plain_text(os.getenv("ECOM_DB_PASSWORD"))
        )

        open_weather_api_key_secret = sm.Secret(self, "OWAPIKey",
            removal_policy=remove.DESTROY,
            secret_name=other_config["api_key_path"],
            secret_string_value=SecretValue.unsafe_plain_text(os.getenv("OW_API_KEY"))
        )

        '''
        We're generating a key here, and storing it, which probably isn't a great implementation
        A better long term solution would probably be a dedicated lamda for creation/rotation
        Or realistically do some manual preprovisioning (though that can lead to other future issues.)
        '''
        fernet_key = Fernet.generate_key().decode()

        fernet_key_secret = sm.Secret(self, "FernetKey",
            removal_policy=remove.DESTROY,
            secret_name=airflow_config["fernetKeySecretName"],
            secret_string_value=SecretValue.unsafe_plain_text(fernet_key)
        )

        #initialize infrastructure/networking
        ecs_vpc =  ec2.Vpc(self, "VedaVPC",
            vpc_name="veda-pipeline-vpc",
            ip_addresses=ec2.IpAddresses.cidr("10.0.0.0/16"),
            max_azs=1
        )
        
        #Cloudmap to find services
        cm_airflow_namespace = cloudmap.PrivateDnsNamespace(self, "airflow-namespace",
            vpc=ecs_vpc,
            name=cloudmap_config["namespace"]        
        )

        #TODO: Need real subnets
        db_subnet_group = rds.SubnetGroup(self, "VedaDBSubnetGroup", 
            description="description",
            vpc=ecs_vpc
        )

        redis_subnet_group = elasticache.CfnSubnetGroup(self, "VedaRedisSubnetGroup",
            description="subnet group for redis",
            subnet_ids=ecs_vpc.select_subnets(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS).subnet_ids
        )

        task_execution_role_arn = f'arn:aws:iam::{cloud_env.account}:role/ecsTaskExecutionRole'
        task_execution_role = iam.Role.from_role_arn(self, 'taskExecutionRole', task_execution_role_arn)

        #TODO: Better Security
        ecs_task_sg = ec2.SecurityGroup(self, 'taskSecurityGroup', 
            vpc=ecs_vpc,
        )

        redis_sg = ec2.SecurityGroup(self, "redis-sec-group",
            security_group_name="redis-sg", 
            vpc=ecs_vpc, 
            allow_all_outbound=True ,
        )

        airflow_task_role = iam.Role(self, "airflowTaskRole",
            description="Role for airflow to access S3 for logs",
            assumed_by= iam.ServicePrincipal("ecs-tasks.amazonaws.com")
        )

        #Elasitcache Redis (not serverless)
        redis_cluster = elasticache.CfnCacheCluster(self, "airflow_redis",
            engine="redis",
            cache_node_type="cache.t3.small",
            num_cache_nodes=1,
            cache_subnet_group_name=redis_subnet_group.ref,
            vpc_security_group_ids=[redis_sg.security_group_id],
        )  

        #Airflow DB (Postgres Serverless)
        aurora = rds.CfnDBCluster(self, 'AuroraAirflow',
            database_name=db_config["databaseName"],
            db_cluster_identifier=db_config["dbClusterIdentifier"],
            engine=db_config["engine"],
            engine_mode=db_config["engineMode"],
            master_username=db_config["masterUsername"],
            master_user_password=db_password_secret.secret_value.unsafe_unwrap(),
            port=db_config["port"],
            db_subnet_group_name=db_subnet_group.subnet_group_name,
            vpc_security_group_ids=[ecs_task_sg.security_group_id],
            serverless_v2_scaling_configuration=rds.CfnDBCluster.ServerlessV2ScalingConfigurationProperty(
                max_capacity=db_config["maxCapacity"],
                min_capacity=db_config["minCapacity"]
            ),
        )

        #Airflow Docker
        airflow_image = DockerImageAsset(self, "airflowImage",
            directory="./docker"
        )

        #Airflow Cluster
        ecsCluster = ecs.Cluster(self, "airflowCluster",
            vpc=ecs_vpc
        )

        #S3 bucket for Airflow logs, and access grant
        airflow_bucket = s3.Bucket(self, "AirflowBucket",
            bucket_name="kcypher-veda-airflow-bucket",
            versioned=True,
            removal_policy=remove.DESTROY,
            encryption=s3.BucketEncryption.S3_MANAGED
        )
        airflow_bucket.grant_read_write(airflow_task_role)
        s3_log_path = "s3://" + airflow_bucket.bucket_name + "/logs"

        #ECS Service - Webserver (webserver.airflow within VPC)
        airflow_web_task = ecs.FargateTaskDefinition(self, "webserverTask",
            family="airflow_webserver",
            #These are purely guesses TODO:Quantify requirements
            cpu=512,
            memory_limit_mib=1024,
            execution_role=task_execution_role,
            task_role=airflow_task_role #these could probably be named better
        )
        
        airflow_web_task.add_container('DefaultContainer',
            image=ecs.ContainerImage.from_registry(airflow_image.image_uri),
            command=['webserver'],
            #TODO:This logging config is probably suboptimal, but I don't want a surprise cloudwatch bill next year
            logging=ecs.AwsLogDriver(stream_prefix="airflow-webserver", log_retention=logs.RetentionDays.ONE_DAY),
            environment={
                "LOAD_EX": airflow_config["loadExamples"],
                "POSTGRES_DB": db_config["databaseName"],
                "POSTGRES_HOST": aurora.attr_endpoint_address,
                "POSTGRES_PORT": aurora.attr_endpoint_port,
                "POSTGRES_USER": db_config["masterUsername"],
                "AIRFLOW__CORE__REMOTE_BASE_LOG_FOLDER": s3_log_path,
                "REDIS_HOST": redis_cluster.attr_redis_endpoint_address,
                "REDIS_PORT": redis_config["port"]
            },
            secrets={
                "POSTGRES_PASSWORD": ecs.Secret.from_secrets_manager(db_password_secret),
                "REDIS_PASSWORD": ecs.Secret.from_secrets_manager(redis_password_secret),
                "FERNET_KEY": ecs.Secret.from_secrets_manager(fernet_key_secret)
            },
            port_mappings=[ecs.PortMapping(container_port=8080)]
        )

        airflow_web_service = ecs.FargateService(self, "webserverService",
            service_name="airflow-webserver",
            cluster=ecsCluster,
            task_definition=airflow_web_task,
            desired_count=1,
            security_groups=[ecs_task_sg],
            assign_public_ip=False,
            cloud_map_options=ecs.CloudMapOptions(
                name=cloudmap_config["webserverServiceName"],
                cloud_map_namespace=cm_airflow_namespace,
                dns_record_type=cloudmap.DnsRecordType.A,
                dns_ttl=Duration.seconds(30)
            )
        )

        #ECS Service - Airflow Scheduler
        airflow_scheduler_task = ecs.FargateTaskDefinition(self, "schedulerTask",
            family="airflow_scheduler",
            cpu=512,
            memory_limit_mib=2024,
            execution_role=task_execution_role,
            task_role=airflow_task_role #these could probably be named better
        )
        
        airflow_scheduler_task.add_container('DefaultContainer',
            image=ecs.ContainerImage.from_registry(airflow_image.image_uri),
            command=['scheduler'],
            logging=ecs.AwsLogDriver(stream_prefix="airflow-scheduler", log_retention=logs.RetentionDays.ONE_DAY),
            environment={
                "LOAD_EX": airflow_config["loadExamples"],
                "POSTGRES_DB": db_config["databaseName"],
                "POSTGRES_HOST": aurora.attr_endpoint_address,
                "POSTGRES_PORT": aurora.attr_endpoint_port,
                "POSTGRES_USER": db_config["masterUsername"],
                "AIRFLOW__CORE__REMOTE_BASE_LOG_FOLDER": s3_log_path,
                "REDIS_HOST": redis_cluster.attr_redis_endpoint_address,
                "REDIS_PORT": redis_config["port"]
            },
            secrets={
                "POSTGRES_PASSWORD": ecs.Secret.from_secrets_manager(db_password_secret),
                "REDIS_PASSWORD": ecs.Secret.from_secrets_manager(redis_password_secret),
                "FERNET_KEY": ecs.Secret.from_secrets_manager(fernet_key_secret)
            }
        )

        airflow_scheduler_service = ecs.FargateService(self, "schedulerService",
            service_name="airflow-scheduler",
            cluster=ecsCluster,
            task_definition=airflow_scheduler_task,
            desired_count=1,
            security_groups=[ecs_task_sg],
            assign_public_ip=False
        )

        #ECS Service - Flower
        airflow_flower_task = ecs.FargateTaskDefinition(self, "flowerTask",
            family="airflow_flower",
            cpu=256,
            memory_limit_mib=512,
            execution_role=task_execution_role,
            task_role=airflow_task_role #these could probably be named better
        )
        
        airflow_flower_task.add_container('DefaultContainer',
            image=ecs.ContainerImage.from_registry(airflow_image.image_uri),
            command=['flower'],
            logging=ecs.AwsLogDriver(stream_prefix="airflow-flower", log_retention=logs.RetentionDays.ONE_DAY),
            environment={
                "LOAD_EX": airflow_config["loadExamples"],
                "POSTGRES_DB": db_config["databaseName"],
                "POSTGRES_HOST": aurora.attr_endpoint_address,
                "POSTGRES_PORT": aurora.attr_endpoint_port,
                "POSTGRES_USER": db_config["masterUsername"],
                "AIRFLOW__CORE__REMOTE_BASE_LOG_FOLDER": s3_log_path,
                "REDIS_HOST": redis_cluster.attr_redis_endpoint_address,
                "REDIS_PORT": redis_config["port"]
            },
            secrets={
                "POSTGRES_PASSWORD": ecs.Secret.from_secrets_manager(db_password_secret),
                "REDIS_PASSWORD": ecs.Secret.from_secrets_manager(redis_password_secret),
                "FERNET_KEY": ecs.Secret.from_secrets_manager(fernet_key_secret)
            },
            port_mappings=[ecs.PortMapping(container_port=5555)]
        )

        airflow_flower_service = ecs.FargateService(self, "flowerService",
            service_name="airflow-flower",
            cluster=ecsCluster,
            task_definition=airflow_flower_task,
            desired_count=1,
            security_groups=[ecs_task_sg],
            assign_public_ip=False,
            cloud_map_options=ecs.CloudMapOptions(
                name=cloudmap_config["flowerServiceName"],
                cloud_map_namespace=cm_airflow_namespace,
                dns_record_type=cloudmap.DnsRecordType.A,
                dns_ttl=Duration.seconds(30)
            )
        )

        #ECS Service - Worker
        airflow_worker_task = ecs.FargateTaskDefinition(self, "workerTask",
            family="airflow_worker",
            cpu=1024,
            memory_limit_mib=3072,
            execution_role=task_execution_role,
            task_role=airflow_task_role #these could probably be named better
        )
        
        airflow_worker_task.add_container('DefaultContainer',
            image=ecs.ContainerImage.from_registry(airflow_image.image_uri),
            command=['worker'],
            logging=ecs.AwsLogDriver(stream_prefix="airflow-worker", log_retention=logs.RetentionDays.ONE_DAY),
            #TODO: As I type this the third time, I should probably just use a separate .env file
            environment={
                "LOAD_EX": airflow_config["loadExamples"],
                "POSTGRES_DB": db_config["databaseName"],
                "POSTGRES_HOST": aurora.attr_endpoint_address,
                "POSTGRES_PORT": aurora.attr_endpoint_port,
                "POSTGRES_USER": db_config["masterUsername"],
                "AIRFLOW__CORE__REMOTE_BASE_LOG_FOLDER": s3_log_path,
                "REDIS_HOST": redis_cluster.attr_redis_endpoint_address,
                "REDIS_PORT": redis_config["port"]
            },
            secrets={
                "POSTGRES_PASSWORD": ecs.Secret.from_secrets_manager(db_password_secret),
                "REDIS_PASSWORD": ecs.Secret.from_secrets_manager(redis_password_secret),
                "FERNET_KEY": ecs.Secret.from_secrets_manager(fernet_key_secret)
            },
            port_mappings=[ecs.PortMapping(container_port=8793)] #TODO: Un-magic these port numbers
        )

        airflow_worker_service = ecs.FargateService(self, "workerService",
            service_name="airflow-worker",
            cluster=ecsCluster,
            task_definition=airflow_worker_task,
            desired_count=1, #This should probably be higher for workers in a real environment.
            security_groups=[ecs_task_sg],
            assign_public_ip=False
        )

        #Past this is Data Pipelining Stuff. This should probably be a separate stack
        green_coffee_products_table = dynamodb.TableV2(self, "GreenCoffeeProduct", 
            table_name="GreenCoffeeProduct",                                        
            partition_key=dynamodb.Attribute(name="id", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="has_been_purchased", type=dynamodb.AttributeType.NUMBER),
            local_secondary_indexes=[dynamodb.LocalSecondaryIndexProps(
                    index_name="geocoded",
                    sort_key=dynamodb.Attribute(name="has_been_purchased", type=dynamodb.AttributeType.NUMBER)
                )
            ],
            removal_policy=remove.DESTROY,
        )

        #Ecommerce side (normalized products)
        aurora = rds.CfnDBCluster(self, 'CoffeeEcomDB',
            database_name=ecommerce_config["databaseName"],
            db_cluster_identifier=ecommerce_config["dbClusterIdentifier"],
            engine=ecommerce_config["engine"],
            engine_mode=ecommerce_config["engineMode"],
            master_username=ecommerce_config["masterUsername"],
            master_user_password=db_password_secret.secret_value.unsafe_unwrap(),
            port=ecommerce_config["port"],
            db_subnet_group_name=db_subnet_group.subnet_group_name,
            vpc_security_group_ids=[ecs_task_sg.security_group_id],
            serverless_v2_scaling_configuration=rds.CfnDBCluster.ServerlessV2ScalingConfigurationProperty(
                max_capacity=ecommerce_config["maxCapacity"],
                min_capacity=ecommerce_config["minCapacity"]
            ),
        )





