import boto3
import datetime
import yaml
import re

def load_config():
    return yaml.load(open("config.yaml", "r", encoding="utf-8"), Loader=yaml.SafeLoader)
    
# ALBの存在チェック
def exist_alb(name):
    client = boto3.client('elbv2')
    try:
        client.describe_load_balancers(Names=[name])
        return True
    except:
        return False

# ELBの存在チェック
def exist_elb(name):
    client = boto3.client('elb')
    try:
        client.describe_load_balancers(LoadBalancerNames=[name])
        return True
    except:
        return False

# ALBまたはELBが存在するなら文字列を、そうでなければ例外を送出
def get_loadbalancer(name):
    if exist_alb(name):
        return "ALB"
    
    if exist_elb(name):
        return "ELB"
    
    raise Exception(f"{name} not found")

# Athenaでクエリを実行する。スキーマ名、結果の転送先は config.yaml で指定する
def exec_athena_query(query):
    config = load_config()
    timestamp = datetime.datetime.now().isoformat()

    client = boto3.client('athena')
    response = client.start_query_execution(
        QueryString=query,
        ClientRequestToken=f"query_{timestamp}",
        QueryExecutionContext={
            'Database': config.schema
        },
        ResultConfiguration={
            'OutputLocation': config.resultS3location,
            'EncryptionConfiguration': {
                'EncryptionOption': 'SSE_S3'
            }
        }
    )

# 指定されたパスにアクセスできて、オブジェクトが1個以上存在するならTrue
def get_some_objects_from_s3(s3path):
    match = re.search(r"s3://(?P<bucket>[^/]+)(?P<prefix>/?.*)", s3path)
    if match == None:
        raise Exception(f"malformed s3 path: {s3path}")
    
    bucket_name = match.group("bucket")
    prefix = match.group("prefix")
    if prefix == "/":
        prefix = ""

    try:
        client = boto3.client('s3')
        objects = client.list_objects_v2(Bucket=bucket_name, MaxKeys=1, Prefix=prefix)
        return objects["KeyCount"] != 0
    except:
        return False

# 使い方を出力
def print_usage():
    this_year = timestamp = datetime.datetime.now().year

    print(f"[USAGE]")
    print(f"table.py <lb-name> <s3path> [year(={this_year})] [dryRunMode]")
    print(f"")
    print(f"[example 1]")
    print(f"table.py myAlb s3://.../ap-northeast-1/")
    print(f"  - create athena table for this year({this_year})")
    print(f"")
    print(f"[example 2]")
    print(f"table.py myAlb s3://.../ap-northeast-1/ 2019")
    print(f"  - create athena table for year=2019")
    print(f"")
    print(f"[example 2]")
    print(f"table.py myAlb s3://.../ap-northeast-1/ 2020 1")
    print(f"  - dry-run mode (just print DDL)")
    print(f"")
