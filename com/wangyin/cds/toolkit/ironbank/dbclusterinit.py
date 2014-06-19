__author__ = 'zy'
import re
from com.wangyin.cds.toolkit.rest.cds import CdsClient


regex_create_table = re.compile('create\\s+table\\s+([^\\s]+)', re.IGNORECASE)
regex_comment = re.compile('/\\*.+\\*/', re.DOTALL)


def create_table(sql, cluster):
    cds = CdsClient()

    cluster_obj = cds.getCluster(cluster)
    adjusted_stmts = []
    for raw_stmt in sql.split(';'):
        stmt = raw_stmt.strip()
        if len(stmt) == 0 or regex_comment.match(stmt):
            continue
        #is a create statement??
        matched = regex_create_table.match(stmt)
        if matched:
            table_name = re.sub(r'^`(.+)`$', '\\1', matched.group(1))
            #create on global_group first
            exec_on_group(cluster_obj.global_group, stmt)

            dispatch_groups = cds.get_grpid2_tblsuf(cluster, table_name)
            if dispatch_groups:
                #print("table %s is a splitting table, need to create slice"%table_name)
                create_inverted_index(cluster_obj, table_name)
                for gid, ts in dispatch_groups:
                    group_obj = find_obj_in_cluster(gid, cluster_obj)
                    if not group_obj:
                        raise Exception("cannot find group with id %d" % gid)
                    #change table name
                    new_tbname = "%s_%s" % (table_name, ts)
                    new_stmt = "CREATE TABLE `" + new_tbname + "`" + stmt[matched.end():]
                    exec_on_group(group_obj, new_stmt)

            else:
                #print("table %s is a global table, keep name"%table_name)
                #create this table on all worker groups
                for gr in cluster_obj.work_groups:
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
        group_ph="`group_%d` int(20) DEFAULT '0'"
        group_col_defs=[]
        for wk in cluster.work_groups:
            group_col_defs.append(group_ph % wk.id)
        group_ddl_part = ','.join(group_col_defs)
        whole_ddl = sql_ddl %(table_name.upper(),group_ddl_part)
        exec_on_group(cluster.global_group,whole_ddl)

def find_obj_in_cluster(self,gid, cluster):
        for gr in cluster.work_groups:
            if gr.id == gid:
                return gr
        return None

def exec_on_group(group, sql):
    #print("on group %s, execute sql:" % group.name)
    #no difference to execute between master and slave
    exec_on_db(group.master, sql)
    for dd in group.slave:
        exec_on_db(dd, sql)
    #print("\t%s\n"%sql)
    pass


def exec_on_db(db, sql):
    db_conn = new_group_conn(db.ip, db.port, db.schema)
    if not db_conn:
        print("cannot connect to host %s:%s:/%s", (db.ip, db.port, db.schema))
    else:
        try:
            nc = db_conn.cursor()
            nc.execute(sql)
            #print("exec....")
            nc.close()
        except Error as e:
            print(e)
    db_conn.close()