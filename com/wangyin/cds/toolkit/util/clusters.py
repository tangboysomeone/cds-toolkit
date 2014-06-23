# -*- coding: utf-8 -*-
__author__ = 'zy'

from mysql.connector import Error
from datetime import datetime


class ClusterUtil:

    def create_sample_cluster(self, conn):
        try:
            cursor = conn.cursor()
            sql_insert_cluster = "insert into dbcluster(cluster_name) values('sample')"
            sql_insert_group = "insert into dbgroup(group_name,db_type) values(%s, 'mysql')"
            sql_insert_db = "insert into dbinfo(ip,port,db_name) values(%s,%s,%s)"
            sql_db_of_group = "insert into rdbinfodbgroup(db_group_id,db_info_id,master_or_slave) values(%s,%s,'master')"
            sql_group_of_cluster = "insert into rdbclusterdbgroup(db_cluster_id,db_group_id,group_type) values(%s,%s,%s)"
            dt = self.insert_append(sql_insert_cluster)
            cursor.execute(dt)
            cluster_id = cursor.lastrowid

            hosts = self.config.get('group', 'host').split(',')
            ports = self.config.get('group', 'port').split(',')
            dbs = self.config.get('group', 'db').split(',')

            cursor = conn.cursor()
            i = 0
            for ph in hosts:
                #create group
                dt = self.insert_append(sql_insert_group)
                group_name = 'Global WorkGroup' if i == 0 else "WorkGroup%s" % i
                cursor.execute(dt, (group_name,))
                group_id = cursor.lastrowid
                #create master database
                dt = self.insert_append(sql_insert_db)
                pp = ports[i]
                pd = dbs[i]
                cursor.execute(dt, (ph, pp, pd))
                db_id = cursor.lastrowid
                #bind db with group
                cursor.execute(sql_db_of_group, (group_id, db_id))

                #bind group with cluster
                if i == 0:
                    cursor.execute(sql_group_of_cluster, (cluster_id, group_id, 'global'))
                else:
                    cursor.execute(sql_group_of_cluster, (cluster_id, group_id, 'worker'))
                i = i + 1
            conn.commit()
            print("created sample cluster successfully!")
        except Error as e:
            print(e)
            conn.rollback()
        cursor.close()
        conn.close()


    def create_sample_app(self, conn):
        try:
            nc = conn.cursor()
            insert_sql = "insert into app(app_name,owner,phone,email,attribute) values(%s,%s,%s,%s,'none')"
            full_sql = self.insert_append(insert_sql)
            print(full_sql)
            nc.execute(full_sql, ('demo', 'david chan', '18610562313', 'wychenbangyi@chinabank.com.cn'))
            conn.commit()
            print("created demo app successfully!")
            nc.close()
        except Error as e:
            print(e)
            conn.rollback()

        conn.close





