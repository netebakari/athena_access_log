import boto3
import datetime
import yaml
import re

def parse_lb_names(raw_lb_name):
    s = raw_lb_name.split("=", 1)
    if len(s) == 1:
        result = {"lbname": raw_lb_name, "short_name": raw_lb_name}
    else:
        result = {"lbname": s[0], "short_name": s[1]}
    
    match = re.search(r"^[a-zA-Z-_]+$", result["short_name"])
    if match == None:
        raise Exception(f"short name '{result['short_name']}' malformed")
    
    return result

    
# 指定した名前のALBが存在し、ログの保存が有効になっていたらS3の情報を返す。それ以外の場合はNoneを返す
def get_alb(name):
    client = boto3.client('elbv2')
    try:
        alb = client.describe_load_balancers(Names=[name])
        arn = alb["LoadBalancers"][0]["LoadBalancerArn"]
        attributes = client.describe_load_balancer_attributes(LoadBalancerArn=arn)

        bucket = [x for x in attributes["Attributes"] if x["Key"] == "access_logs.s3.bucket"]
        prefix = [x for x in attributes["Attributes"] if x["Key"] == "access_logs.s3.prefix"]
        if len(bucket) == 0 or len(prefix) == 0:
            return None

        return {"type": "ALB", "bucket": bucket[0]["Value"], "prefix": prefix[0]["Value"]}
    except:
        return None

# 指定した名前のClassic ELBが存在し、ログの保存が有効になっていたらS3の情報を返す。それ以外の場合はNoneを返す
def get_elb(name):
    client = boto3.client('elb')
    try:
        attributes = client.describe_load_balancer_attributes(LoadBalancerName=name)
        access_log = attributes["LoadBalancerAttributes"]["AccessLog"]
        if not access_log["Enabled"]:
            return None

        return {"type": "ELB", "bucket": access_log["S3BucketName"], "prefix": access_log["S3BucketPrefix"]}
    except:
        return None

# ALBまたはELBが存在し、ログの保存が有効になっていたらその情報を返す。そうでなければ例外を送出
def get_loadbalancer(name):
    alb = get_alb(name)
    if alb != None:
        return alb

    elb = get_elb(name)    
    if elb != None:
        return elb

    raise Exception(f"{name} not found or access log not enabled")

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
    match = re.search(r"s3://(?P<bucket>[^/]+)/?(?P<prefix>.*)", s3path)
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


# AWSアカウント番号を取得する
def get_account_no():
    client = boto3.client('sts')
    identity = client.get_caller_identity()
    return identity["Account"]


# テーブル作成DDLを取得する
def get_table_creation_ddl(type, tablename, s3_path):
    filename = "ddl_alb" if type == "ALB" else "ddl_elb"
    file = open(f"{filename}.txt")
    content = file.read()
    file.close()
    account_no = get_account_no()
    region = load_config()["region"]

    content = content.replace("$TABLENAME", tablename)
    if prefix == "":
        prefix = "/"
    if prefix[0] != "/":
        prefix = "/" + prefix
    content = content.replace("$S3PATH", s3_path)
    return content

# Classic ELBのテーブルからビューを作るDDLを取得する
def get_transformed_view_ddl(tablename):
    file = open("ddl_elb_view.txt")
    content = file.read()
    file.close()
    content = content.replace("$TABLENAME", tablename)
    return content

# パーティショニングを設定するDDLを取得する
def get_partitioning_ddl(table_name, lb_name, s3_path, year, day_count):
    date = datetime.date(year, 1, 1) + datetime.timedelta(days=day_count)
    return f"ALTER TABLE `{table_name}` ADD PARTITION (albname='{lb_name}', year={year}, month={date.month}, day={date.day}) location '{s3_path}/{date.strftime('%m')}/{date.strftime('%d')}'"


# 処理の本体。1個のALB/Classic ELBに対してテーブル作成、ビュー作成(ELBの場合)、パーティショニング設定を行う
def process(lb_name, short_name, year, dry_run_mode):
    # ALBかclassic ELBかをチェック（見つからなければ例外で終わり）
    loadbalancer = get_loadbalancer(lb_name)
    table_name = f"{short_name}_accesslogs_{year}".replace("-", "_")

    # S3のパスを決める
    prefix = f"/{loadbalancer['prefix']}/".replace("//", "/")
    s3_path = f"s3://{loadbalancer['bucket']}{prefix}AWSLogs/{account_no}/elasticloadbalancing/{region}/{year}"

    # テーブル作る
    query = get_table_creation_ddl(loadbalancer["type"], s3_path)
    print(query)
    print("-- -----------")
    if not dry_run_mode:
        exec_athena_query(query)
        print(f"table {table_name} created")

    # Classic ELBのときはビューも作る
    if loadbalancer["type"] == "ELB":
        query = get_transformed_view_ddl(table_name)
        print(query)
        print("-- ------------------")
        if not dry_run_mode:
            exec_athena_query(query)
            print(f"view {table_name}_t created")

    # パーティショニング実行
    for i in range(365):
        query = get_partitioning_ddl(table_name, short_name, s3_path, year, i)
        print(query)
        if not dry_run_mode:
            exec_athena_query(query)
            time.sleep(1)

        # 年末に来たらおしまい
        if date.month == 12 and date.day == 31:
            break
