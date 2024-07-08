from diagrams import Cluster, Diagram
from diagrams.aws.compute import Fargate, Lambda
from diagrams.aws.database import RDS, Redshift, ElasticacheForRedis, Neptune
from diagrams.aws.iot import IotEvents
from diagrams.aws.storage import S3
from diagrams.aws.network import ALB, InternetGateway
from diagrams.aws.security import SecretsManager

#Hack for Windows
import os
os.environ["PATH"] += os.pathsep + 'C:/Program Files/Graphviz/bin/'

#TODO: Change to neato and make cleaner.
with Diagram("Veda Serverless CFN"):
    with Cluster("Sources"):
        users = InternetGateway("User Transactions")
        scraper = Lambda("Serverless Scrapers")
        iot = IotEvents("Roaster IoT Data")

    with Cluster("AWS Fargate"):
        with Cluster("Scaling Workers"):
            scale = Fargate()
            that = Fargate()
            workers = Fargate()

        redis = ElasticacheForRedis("Redis")
        flower = Fargate("Flower")
        scheduler = Fargate("Scheduler")
        webserver = Fargate("Webserver")

    with Cluster("Outputs"):
        store = S3("events store")
        dw = Redshift("Data Warehouse")
        graph = Neptune("Recommendation Graph")
    
    rds = RDS("Postgres (Serverless)")

    SecretsManager("Passwords, Fernet, etc.")
    logging = S3("Airflow Logging")
    
    [scheduler, webserver, flower, workers] >> logging
    scraper >> webserver >> scale
    scraper >> flower >> scheduler >> workers
    flower >> redis >> that
    rds >> scheduler >> redis
    webserver >> rds >> scale
    that >> dw
