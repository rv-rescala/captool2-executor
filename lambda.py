from catswalk.scraping.webdriver import CWWebDriver
import time
from catswalk.scraping.types.type_webdriver import *
import boto3
import os
import json
from captool.order import *
import csv
import os
from datetime import datetime
from lambda_actor.actor_driver import *
from lambda_actor.actor_executor import *
from lambda_actor.types.type_conf import ActorConf
from lambda_actor.types.type_actor_message import *
from regoogle.drive import *

BUCKET = "captool-gatsby"
GDRIVE_CONF_PATH = "conf/google/gdrive.json"

def get_id_by_key(gdrive, key: str):
    kis = gdrive.list_key_id()
    print(kis)
    for ki in kis:
        _key = ki["name"]
        id = ki["id"]
        if key == _key:
            return id
    return None

def gdrive_init():
    # gconf
    gconf = "/tmp/gdrive.json"
    s3_client = boto3.client('s3')
    s3_client.download_file(BUCKET, GDRIVE_CONF_PATH, "/tmp/gdrive.json")
    
    with open(gconf, 'r') as f:
        gconf_j = json.load(f)
        key_name = gconf_j["key"]
        parents = gconf_j["folder"]
        print(f"gdrive_init, {key_name}, {parents}")
        local_key = f"/tmp/{key_name}"
        s3_client.download_file(BUCKET, f"conf/google/{key_name}", f"/tmp/{key_name}")
    return GoogleDrive(local_key, parents)
    
def gdrive_upload(gdrive, local_fullpath, parents=None):
    filename = local_fullpath.split("/")[-1]
    status = gdrive.upload_file(filename=filename, local_path=local_fullpath, parents=parents)
    os.remove(local_fullpath)
    return status

def s3_upload(local_fullpath, order):
    filename = local_fullpath.split("/")[-1]
    s3_client = boto3.client('s3')
    s3_client.upload_file(local_fullpath, BUCKET, f"dataset/output/{order}/{filename}")
    os.remove(local_fullpath)
    print(f"s3_upload: {local_fullpath} to dataset/output/{order}/{filename}")
    
    #s3_client.upload_file(f"/tmp/{order}/tmp_{filename}", BUCKET, f"dataset/output/{order}/tmp_{filename}")
    #os.remove(f"/tmp/{order}/tmp_{filename}")
    #print(f"s3_upload_tmp : /tmp/{order}/tmp_{filename}")
    
    return f"s3://{BUCKET}/dataset/output/{order}/{filename}"
    
def get_input_list(order) -> str:
    s3_client = boto3.client('s3')
    tmp_path = f"/tmp/test_{order}.csv"
    # get csv
    s3_client.download_file(BUCKET, f"dataset/input/{order}.csv", tmp_path)
    with open(tmp_path, "r") as f:
        reader = csv.reader(f)
        input_list =  [row for row in reader]
    return input_list
    
def grammar_path() -> str:
    s3_client = boto3.client('s3')
    tmp_path = f"/tmp/grammar.lark"
    s3_client.download_file(BUCKET, f"conf/common/grammar.lark", tmp_path)
    return tmp_path
    
def order_path(order) -> str:
    s3_client = boto3.client('s3')
    tmp_path = f"/tmp/{order}_command.od"
    s3_client.download_file(BUCKET, f"conf/order/{order}/command.od", tmp_path)
    return tmp_path
    
def device(order) -> str:
    s3_client = boto3.client('s3')
    tmp_path = f"/tmp/{order}_browser.json"
    s3_client.download_file(BUCKET, f"conf/order/{order}/browser.json", tmp_path)
    # order specific setting
    with open(tmp_path, "r") as f:
        j = json.load(f)
        device = DEVICE.str_to_enum(j["device"])
    return device

"""
        executor_trigger_message_str = None
        # for debug
        request = CWWebDriver(execution_env=EXECUTION_ENV.AWS_LAMBDA, device = device("unext_list"))
"""


def handler(event, context):
    #try:
    executor_trigger_message_str = event["Records"][0]["body"]
    print(executor_trigger_message_str)
    executor_trigger_message = ExecutorTriggerMessage.decode(executor_trigger_message_str)
    # gdrive init
    #gdrive = gdrive_init()
            
    def execution_func(task_message):
        request = CWWebDriver(execution_env=EXECUTION_ENV.AWS_LAMBDA, device = device(task_message.task_groupid))
        output_path = f"/tmp"
        message = task_message.message
        url = message.split(",")[1]
        filename = message.split(",")[2]
        order_name = task_message.task_groupid
        print(f"execution_func: {url},{filename},{order_name}")
        path = execute(request=request, order_name=order_name, grammar_path=grammar_path(), order_path=order_path(order_name), url=url,output_path=output_path, filename=filename)
        print(f"execution_func: {path}")
        s3_upload(path, order_name)
        request.close()
        #parents = get_id_by_key(gdrive, order_name)
        #gdrive_upload(gdrive, path, parents)
        

    def success_func(message):
        print(f"success_func: {message}")
        return f"successed: {message}"
    
    def failed_func(message):
        print(f"failed_func: {message}")
        return f"failed: {message}"

    actor_executor(bucket=BUCKET, prefix="conf", actor_conf_file="actor_conf.json", execution_func=execution_func, success_func=success_func, failed_func=failed_func, executor_trigger_message_str=executor_trigger_message_str)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
