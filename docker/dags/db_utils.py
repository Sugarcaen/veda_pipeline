import json
import logging
import time
from typing import Iterator
from sqlalchemy import create_engine

import boto3
from boto3.dynamodb.conditions import Attr

from cloud_utils import S3_DIR

MAX_ATTEMPTS = 3

def put_items_in_dynamo(table_name:str, items:Iterator)->None:
    logging.info(f"writing {len(items)} items to dynamodb table {table_name}")
    dynamodb = boto3.resource('dynamodb')  
    try:
        #Check if the table exists
        table = dynamodb.Table(table_name)
        with table.batch_writer() as writer:
            for item in items:
                writer.put_item(Item=item)

    except dynamodb.meta.client.exceptions.ResourceNotFoundException:
        logging.warn(f"Table {table_name} Not Found")
    except Exception as err:
        logging.fatal(f"Unexpected {err=}, {type(err)=}")        
    
def ingest_redshift_data(table:str, s3_path:str)->None:

    #Ingest S3 data into redshift directly
    raise NotImplementedError

def import_s3_file(file_name:str)->None:
    load_data_ddl = f"""
        COPY employee 
        FROM '{S3_DIR}/{file_name}' 
        DELIMITER ','
        IGNOREHEADER as 1;
    """
    run_redshift_statement(load_data_ddl)
    logging.info('Imported S3 file to Redshift.')

#Generalized redshift DDL runner
def run_redshift_statement(sql_statement):
    client = client('redshift-data')
    res = client.execute_statement(
        Database="ecom_usage",
        Sql=sql_statement
    )

    done = False
    attempts = 0

    while not done and attempts < MAX_ATTEMPTS:
        attempts += 1
        time.sleep(1)
        desc = client.describe_statement(Id=res['Id'])
        query_status = desc['Status']

    if query_status == "FAILED":
        raise Exception('SQL query failed: ' + desc["Error"])

    match query_status:
        case "FAILED":
            raise Exception('SQL query failed: ' + desc["Error"])
        case "FINISHED":
            done = True
            has_result_set = desc['HasResultSet']
        case _:
            logging.info("Current working... query status is: {} ".format(query_status))

    if not done and attempts >= MAX_ATTEMPTS:
        raise Exception('Maximum of ' + str(attempts) + ' attempts reached.')

def push_products_to_graph():
    #Get new products and update Neptune graph
    raise NotImplementedError

def push_purchases_to_graph():
    #get completed transactions and update grpah
    raise NotImplementedError

def push_roasting_profile_to_graph():
    #get IoT data from s3 and push to graph
    raise NotImplementedError

def update_product_to_purchased(id:str):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('GreenCoffeeProduct')
    try:
        table.update_item(
            Key={"id": id},
            UpdateExpression="set has_been_purchased=:true",
            ExpressionAttributeValues={":true": 1},
            ReturnValues="UPDATED_NEW",
        )
    except Exception as err:
        logging.error(f"Unexpected {err=}, {type(err)=}")      


def get_new_products():
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('GreenCoffeeProduct')
    try:
        items = table.scan(
            FilterExpression=Attr('has_been_purchased').eq(0)
        )
        return items["Items"]
    except Exception as e:
        logging.error(e)