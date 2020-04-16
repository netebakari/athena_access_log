import lib
import sys
import datetime

if len(sys.argv) < 3:
    lib.print_usage()
    sys.exit(1)

this_year = timestamp = datetime.datetime.now().year
lb_name = sys.argv[1]
s3_path = sys.argv[2]
year = this_year if len(sys.argv) <= 3 else int(sys.argv[3])
dry_run_mode = len(sys.argv) >= 5

lb_type = lib.get_loadbalancer(lb_name)
if (lib_type == "ALB"):
    print("ALB!!")
else:
    print("ELB!!")

def get_table_creation_ddl(type, tablename, s3path):
    file = open(f'{"ddl_alb" if type == "ALB" else "ddl_elb"}.txt')
    content = file.read()
    file.close()
    content = content.replace("$TABLENAME", tablename)
    content = content.replace("$S3PATH", s3path)
    return content
