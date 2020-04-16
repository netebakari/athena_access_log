import lib
import sys
import datetime

if len(sys.argv) < 2:
    lib.print_usage()
    sys.exit(1)

this_year = timestamp = datetime.datetime.now().year
lb_name = sys.argv[1]
year = this_year if len(sys.argv) <= 2 else int(sys.argv[2])
dry_run_mode = len(sys.argv) >= 4


# ALBかclassic ELBかをチェック（見つからなければ例外で終わり）
loadbalancer = lib.get_loadbalancer(lb_name)
table_name = f"{lb_name}_accesslogs_{year}"

if dry_run_mode:
    print("--------------------")
    query = lib.get_table_creation_ddl(loadbalancer["type"], table_name, loadbalancer["bucket"], loadbalancer["prefix"])
    print(query)
    if loadbalancer["type"] == "ELB":
        print("--------------------")
        query = lib.get_transformed_view_ddl(table_name)
        print(query)
    print("--------------------")
    sys.exit(0)

