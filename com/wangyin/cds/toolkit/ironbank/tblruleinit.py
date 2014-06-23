__author__ = 'zy'
import argparse
import datetime
from mysql.connector import Error
from mysql import connector
from com.wangyin.cds.toolkit.util.clusters import ClusterUtil
from com.wangyin.cds.toolkit.rest.cds import CdsClient


def init(cluster_name):
    parser = argparse.ArgumentParser(description='Table Rule Init Command Description')
    parser.add_argument('--S', help='the host of CDS-SERVER', default='127.0.0.1')
    parser.add_argument('--restport', help='the rest interface port', default=8088, type=int)
    parser.add_argument('--eventport', help='the event port', default=8888, type=int)
    parser.add_argument('--h', help='the cluster management db ip', default='127.0.0.1')
    parser.add_argument('--P', help='the cluster management db port', default=3306, type=int)
    parser.add_argument('--u', help='the cluster management db username')
    parser.add_argument('--p', help='the cluster management db password')
    parser.add_argument('--database', help='the cluster management db password')
    parser.add_argument('--max', help='the max limit of range', default=1000000, type=int)
    parser.add_argument('--slices', help='the slices per group', default=4, type=int)
    args = parser.parse_args()

    cds = CdsClient(args.h, args.restport, args.eventport)

    try:
        cds = CdsClient(args.h, args.restport, args.eventport)
        db_cluster = cds.getCluster(cluster_name)
        if not db_cluster:
            print 'DbCluster doest\'t exist clusterName: {}'.format(cluster_name)
            return

        work_groups = db_cluster.work_groups
        split_keys = cds.get_splitkey(db_cluster.id)

        range_key_id = None
        hash_key_id = None

        for split_key in split_keys:
            if split_key.type == 'range':
                range_key_id = split_key.id
            elif split_key.type == 'hash':
                hash_key_id = split_key.id

        if (not range_key_id) or (not hash_key_id):
            print r"there is no legal type( range or hash ) exists in this cluseter!"
            return

        #insert rules
        bind_key_rule = "insert into rrulessplittingkey(splitting_key_id,depots_table_rules_id) values(%s,%s)"

        conn = connector.connect(user=args.u, password=args.p, host=args.h, port=args.P, database=args.database)
        cursor = conn.cursor()

        range_rule_ids = __init_rangrule(cursor, work_groups, args.max, args.slices)
        hash_rule_ids = __init_hashrule(cursor, work_groups, args.slices)

        #bind rules with split_key
        for rule_id in range_rule_ids:
            conn.execute(bind_key_rule, (range_key_id, rule_id))
        for rule_id in hash_rule_ids:
            conn.execute(bind_key_rule, (range_key_id, rule_id))
        conn.commit()
        cursor.close()
        print("creating sample splitting key and corresponding rules successfully!")
    except Error as e:
        print(e)
        conn.rollback()


def _insert_append(sql):
    append_columns = ",create_by, creation_date, modified_by, modification_date)"
    sn = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    v = ",'zy','%s','zy','%s')" % (sn, sn)
    sql_parts = sql.split(')')
    sql_parts.insert(1, append_columns)
    sql_parts.append(v)
    return ' '.join(sql_parts)


def __init_rangrule(cursor, work_groups, max, slices):
    """init range rule
    """
    range_rule_ids = []
    group_count = len(work_groups)
    insert_rule_sql = _insert_append(
        "insert into depotstablerules(rule_type,db_group_id,table_suffix,upper_limit,lower_limit,hash_value) values(%s,%s,%s,%s,%s,%s)")

    for j in range(group_count):
        group_load = (max - j * (max / group_count)) if j == group_count - 1 else max / group_count
        group_id = work_groups[0].id

        #create slices for each group
        for k in range(slices):
            slice_load = (group_load - k * (group_load / slices)) if k == slices - 1 else group_load / slices
            table_suffix = '{0:03d}'.format(j * slices + k + 1)
            new_offset = offset + slice_load
            cursor.execute(insert_rule_sql, ('range', group_id, table_suffix, new_offset, offset + 1, 0))
            offset = new_offset

    return range_rule_ids


def __init_hashrule(cursor, work_groups, slices):
    hash_rule_ids = []
    group_count = len(work_groups)
    group_id = work_groups[0].id
    insert_rule_sql = _insert_append(
        "insert into depotstablerules(rule_type,db_group_id,table_suffix,upper_limit,lower_limit,hash_value) values(%s,%s,%s,%s,%s,%s)")

    for j in range(group_count):
        #create slices for each group
        for k in range(slices):
            table_suffix = '{0:03d}'.format(j * slices + k + 1)
            cursor.execute(insert_rule_sql, ('hash', group_id, table_suffix, 0, 0, __hash32shift(group_id)))
            hash_rule_ids.append(cursor.lastrowid)

    return hash_rule_ids


#Tomas Wang ÕûÊýhashËã·¨
def __hash32shift(key):
    key = ~key + (key << 15)  # key = (key << 15) - key - 1;
    key = key ^ (key >> 12)
    key = key + (key << 2)
    key = key ^ (key >> 4)
    key = key * 2057  #key = (key + (key << 3)) + (key << 11);
    key = key ^ (key >> 16)
    return key


