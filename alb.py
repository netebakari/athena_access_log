import lib
import sys
import datetime
import argparse

this_year = timestamp = datetime.datetime.now().year

parser = argparse.ArgumentParser(description="ALB Log Athena Table Generator")
parser.add_argument("--albnames", help="ALB or Classic ELB names (not ARN). you can provide one more names at a time", required=True,  nargs="+")
parser.add_argument("--result", help="S3 location where query result pushed. must start with 's3://...'", required=True)
parser.add_argument("--region", help="[optional] region. default value is 'ap-northeast-1'", default="ap-northeast-1")
parser.add_argument("--database", help="[optional] dabatase(shcmea) name of AWS Athena. default value is 'default'", default="default")
parser.add_argument("--year", help=f"[optional] year. default value is {this_year}", type=int, default=this_year)
parser.add_argument("--create-unioned-view", help="creates a view of all table", action='store_true')
parser.add_argument("--go", help="execute query. without this option, this script just prints query and exit", action='store_true')
args = parser.parse_args()

lb_names = [lib.parse_lb_names(x) for x in args.albnames]
year = args.year
view = args.create_unioned_view
if year < 2000:
    raise Exception("year must be greater than 2000")
config = {
    "database": args.database,
    "result_s3_location": args.result,
    "region": args.region,
    "dry_run_mode": not args.go,
    "account_no": lib.get_account_no()
}

for n in lb_names:
    # とりあえず存在確認とログ保存が行われる設定になっていることをチェック。1個でも見つからないものがあったら例外で終了
    lib.get_loadbalancer(n["lbname"])

table_names = []
for n in lb_names:
    t = lib.process(n["lbname"], n["short_name"], year, config)
    table_names.append(t)

if view:
    selects = [f"(SELECT * FROM {x})" for x in table_names]
    content = " UNION ".join(selects)
    query = f"CREATE OR REPLACE VIEW alb_access_logs_{year} AS SELECT * FROM ({content}) X"
    print(query)
    lib.exec_athena_query(config, query)

