import lib
import sys
import datetime

if len(sys.argv) < 3:
    lib.print_usage()
    sys.exit(1)

this_year = timestamp = datetime.datetime.now().year
lb_name = sys.argv[1]
s3path = sys.argv[2]
year = this_year if len(sys.argv) <= 3 else int(sys.argv[3])
dry_run_mode = len(sys.argv) >= 5

# まずS3パスの検証を行う。1個以上オブジェクトがあるかどうかを調べる
if not lib.get_some_objects_from_s3(s3path):
    raise Exception(f"no object found or bucket: {s3path}")

# ALBかclassic ELBかをチェック（見つからなければ例外で終わり）
lb_type = lib.get_loadbalancer(lb_name)
query = get_table_creation_ddl(lb_type, f"#{lb_name}_accesslogs_#{year}", s3path)
print(query)

def get_table_creation_ddl(type, tablename, s3path):
    filename = "ddl_alb" if type == "ALB" else "ddl_elb"
    file = open(f"{filename}.txt")
    content = file.read()
    file.close()
    content = content.replace("$TABLENAME", tablename)
    content = content.replace("$S3PATH", s3path)
    return content

def get_transformed_view_ddl(tablename):
    file = open("ddl_elb_view.txt")
    content = file.read()
    file.close()
    content = content.replace("$TABLENAME", tablename)
    return content
