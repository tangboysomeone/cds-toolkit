__author__ = 'zy'
import argparse
import datetime
from mysql.connector import Error
from com.wangyin.cds.toolkit.rest.cds import CdsClient


def init_tbl_rule(self, cluster_name):
    parser = argparse.ArgumentParser(description='Table Rule Init Command Description')
    parser.add_argument('--h', help='the host of CDS-SERVER', default='127.0.0.1')
    parser.add_argument('--restPort', help='the rest interface port', default=8088, type=int)
    parser.add_argument('--eventPort', help='the event port', default=8888, type=int)
    parser.add_argument('--max', help='the max limit of range', default=1000000, type=int)
    parser.add_argument('--slices', help='the slices per group', default=1000000, type=int)
    args = parser.parse_args()

    try:
        cds = CdsClient(args.h, args.restport, args.eventport)
        db_cluster = cds.getCluster()
        if not db_cluster:
            print 'DbCluster doest\'t exist clusterName: {}'.format(cluster_name)
            return

        work_groups = db_cluster.work_groups
        split_keys = cds.get_splitkey(db_cluster.id)

        range_key_id = None
        hash_key_id = None

        for splitKey in split_keys:
            if splitKey.type == 'range':
                range_key_id = splitKey.id
            elif splitKey.type == 'hash':
                hash_key_id = splitKey.id

        if (not range_key_id) or (not hash_key_id):
            print r"there is no legal type( range or hash ) exists in this cluseter!"
            return

        #insert rules
        bind_key_rule = "insert into rrulessplittingkey(splitting_key_id,depots_table_rules_id) values(%s,%s)"

        conn = self.new_backingstore_conn()
        cursor = conn.cursor()

        range_rule_ids = _init_rangrule(cursor, work_groups, args.max, args.slices)
        hash_rule_ids = _init_hashrule(cursor, work_groups, args.slices)

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
    conn.close




def _insert_append(sql):
    append_columns = ",create_by, creation_date, modified_by, modification_date)"
    sn = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    v = ",'zy','%s','zy','%s')" % (sn, sn)
    sql_parts = sql.split(')')
    sql_parts.insert(1, append_columns)
    sql_parts.append(v)
    return ' '.join(sql_parts)


def _init_rangrule(cursor, work_groups, max, slices):
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


def _init_hashrule(cursor, work_groups, slices):
    hash_rule_ids = []
    group_count = len(work_groups)
    group_id = work_groups[0].id
    insert_rule_sql = _insert_append(
        "insert into depotstablerules(rule_type,db_group_id,table_suffix,upper_limit,lower_limit,hash_value) values(%s,%s,%s,%s,%s,%s)")

    for j in range(group_count):
        #create slices for each group
        for k in range(slices):
            table_suffix = '{0:03d}'.format(j * slices + k + 1)
            cursor.execute(insert_rule_sql, ('hash', group_id, table_suffix, 0, 0, j * slices + k))
            hash_rule_ids.append(cursor.lastrowid)

    return hash_rule_ids