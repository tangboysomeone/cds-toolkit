__author__ = 'zy'
import re
import argparse
from com.wangyin.cds.toolkit.util import iterables
from com.wangyin.cds.toolkit.rest.cds import CdsClient
from mysql.connector import Error
from mysql import connector


regex_create_table = re.compile('create\\s+table\\s+([^\\s]+)', re.IGNORECASE)
regex_comment = re.compile('/\\*.+\\*/', re.DOTALL)


def init(cluster_name, files):
    parser = argparse.ArgumentParser(description='Table Rule Init Command Description')
    parser.add_argument('--h', help='the host of CDS-SERVER', default='127.0.0.1')
    parser.add_argument('--restport', help='the rest interface port', default=8088, type=int)
    parser.add_argument('--eventport', help='the event port', default=8888, type=int)
    args = parser.parse_args()

    all_sql = []
    for f in files:
        all_sql.append(f.read())
        f.close()
    ddl_sql = ';'.join(all_sql)

    cds = CdsClient(args.h, args.restport, args.eventport)
    execute_ddl(cds, cluster_name, ddl_sql)


def execute_ddl(cds, cluster_name, ddl_sql):
    db_cluster = cds.getCluster(cluster_name)
    for raw_stmt in ddl_sql.split(';'):
        stmt = raw_stmt.strip()
        if len(stmt) == 0 or regex_comment.match(stmt):
            continue
        #is a create statement??
        matched = regex_create_table.match(stmt)
        if matched:
            table_name = re.sub(r'^`(.+)`$', '\\1', matched.group(1))
            #create on global_group first
            exec_on_group(db_cluster.global_group, stmt)

            grpid_2_tblsuf = cds.get_grpid2_tblsuf(cluster_name, table_name)
            if grpid_2_tblsuf:
                #print("table %s is a splitting table, need to create slice"%table_name)
                create_inverted_index(db_cluster, table_name)
                for group_id, tbl_suffix in grpid_2_tblsuf:
                    group_obj = iterables.find(db_cluster.work_groups, lambda s: s.id == group_id)
                    if not group_obj:
                        raise Exception("cannot find group with id %d" % group_id)
                    #change table name
                    new_tbname = "%s_%s" % (table_name, tbl_suffix)
                    new_stmt = "CREATE TABLE `" + new_tbname + "`" + stmt[matched.end():]
                    exec_on_group(group_obj, new_stmt)

            else:
                #print("table %s is a global table, keep name"%table_name)
                #create this table on all worker groups
                for gr in db_cluster.work_groups:
                    exec_on_group(gr, stmt)
        else:
            print("!!omitted sql statement %s" % stmt)
    print("successfully execute all DDL statements")


def create_inverted_index(cluster, table_name):
    sql_ddl = """CREATE TABLE `%s$LOOKUP` (
                      `id` int(11) PRIMARY KEY AUTO_INCREMENT,
                      `col_name` varchar(100)  NOT NULL,
                      `col_value` VARCHAR(100),
                      %s)ENGINE=InnoDB DEFAULT CHARSET=utf8
  """
    group_ph = "`group_%d` int(20) DEFAULT '0'"
    group_col_defs = []
    for wk in cluster.work_groups:
        group_col_defs.append(group_ph % wk.id)
    group_ddl_part = ','.join(group_col_defs)
    whole_ddl = sql_ddl % (table_name.upper(), group_ddl_part)
    exec_on_group(cluster.global_group, whole_ddl)


def exec_on_group(group, sql):
    #print("on group %s, execute sql:" % group.name)
    #no difference to execute between master and slave
    exec_on_db(group.master, sql)
    for dbinfo in group.slave:
        exec_on_db(dbinfo, sql)


def exec_on_db(dbinfo, sql):
    db_conn = __build_conn(dbinfo)
    if not db_conn:
        print("cannot connect to host %s:%s:/%s", (dbinfo.ip, dbinfo.port, dbinfo.schema))
    else:
        try:
            nc = db_conn.cursor()
            nc.execute(sql)
            #print("exec....")
            nc.close()
        except Error as e:
            print(e)
        db_conn.close()


def __build_conn(dbinfo):
    try:
        cnx = connector.connect(user=dbinfo.user, password=dbinfo.pwd, host=dbinfo.ip, port=dbinfo.port,
                                database=dbinfo.schema)
        return cnx
    except Error as e:
        print(e)
    return None

