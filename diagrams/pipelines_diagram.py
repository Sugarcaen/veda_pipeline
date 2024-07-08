from diagrams import Diagram, Cluster
from diagrams.saas.cdn import Cloudflare
from diagrams.onprem.analytics import Tableau
from diagrams.onprem.database import Neo4J, MongoDB, MySQL
from diagrams.aws.iot import IotEvents
from diagrams.aws.database import Redshift
from diagrams.onprem.workflow import Airflow
from diagrams.aws.compute import Lambda
from diagrams.programming.framework import React



#Hack for Windows
import os
os.environ["PATH"] += os.pathsep + 'C:/Program Files/Graphviz/bin/'

with Diagram("Roasting Data Pipeline", show=False):
    with Cluster("External Sources"):
        weather = Cloudflare("OpenWeather API")
        sm_coffee = Cloudflare("Sweet Maria's Coffee")
        aq_api = Cloudflare("Open AQ API")

    with Cluster("Internal Sources"):
        roaster = IotEvents("Roaster Profiles")
        generator = Lambda("E-commerce Data\nGenerator")

    orchestrator = Airflow("Pipeline Orchestration")
    products_db = MongoDB("External Products DB")
    internal_site = React("Internal Purchasing Site")
    external_site = React("External E-commerce Site")
    dw = Redshift("Analytics Warehouse")
    tableau = Tableau("Reporting/Finance/Etc.")

    ecom_db = MySQL("E-Commerce Site DB")

    rec_db = Neo4J("Coffee Graph\n(for Recommendations)")

    sm_coffee >> products_db >> internal_site >> [ecom_db, dw, rec_db]
    [roaster, generator] >> dw >> tableau
    [rec_db, ecom_db] >> external_site
    generator >> rec_db


    


