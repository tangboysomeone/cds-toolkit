# coding: utf-8
__author__ = 'zy'
import requests
import json
from com.wangyin.cds.toolkit.domain.module import DbCluster, DbGroup, DbInfo, SplittingKey, ColumnInfo
from requests.exceptions import RequestException


class CdsClient:
    def __init__(self, ip, rest_port, event_port):
        self.ip = ip
        self.port = rest_port
        self.event_url = 'http://{0}:{1}'.format(ip, event_port)
        self.url_prefix = 'http://{0}:{1}'.format(ip, rest_port)
        self.headers = {'Host': '192.168.1.101', 'Connection': 'keep-alive', 'Accept-Encoding': 'gzip',
                        'Poll-Req-Type': 'Req-Check-event'}

    def get_events(self):
        try:
            resp = requests.get(self.event_url, headers=self.headers)
            self.headers['poll-session-id'] = resp.headers['poll-session-id']
            events = json.loads(resp.text)
            return events
        except RequestException as ex:
            print 'Fail to connect cds_server', ex
        else:
            return None

    def get_cluster(self, cluster_name):
        resp = requests.get(self.url_prefix + URLS.CLUSTER.format(cluster_name))

        if not resp.text:
            return None

        cluster = json.loads(resp.text)
        db_cluster = DbCluster(cluster['id'], cluster['clusterName'])
        groups = self.get_group(cluster_name)

        for group in groups:
            if group.type == 'global':
                db_cluster.set_global_group(group)
            if group.type == 'worker':
                db_cluster.add_worker_group(group)
        return db_cluster

    def get_group(self, cluster_name):
        resp = requests.get(self.url_prefix + URLS.DBGROUPBY_CLUSTER.format(cluster_name))

        if not resp.text:
            return None

        db_groups = []
        for group in json.loads(resp.text):
            db_group = DbGroup(group['id'], group['groupName'], group['groupType'])
            db_infos = self.get_dbinfo(db_group.name)
            for db_info in db_infos:
                db_group.add_space(db_info, db_info.is_master)
            db_groups.append(db_group)

        return db_groups

    def get_dbinfo(self, group_name):
        resp = requests.get(self.url_prefix + URLS.DBINFO_GROUP_NAME.format(group_name))
        if not resp.text:
            return None

        ret_db_infos = []

        for db_info in json.loads(resp.text):
            db_info = DbInfo(db_info['id'], db_info['ip'], db_info['port'], db_info['dbName'],
                             db_info['masterOrSlave'] == 'Master')
            ret_db_infos.append(db_info)

        return ret_db_infos

    def get_splitkey(self, cluster_id):
        resp = requests.get(self.url_prefix + URLS.SPLITTINGKEY_BY_CLUSTER_ID.format(cluster_id))
        if not resp.text:
            return None

        split_keys = []
        for split_key in json.loads(resp.text):
            key = SplittingKey(split_key['id'], split_key['splitName'], split_key['clusterId'])
            split_keys.append(key)

        return split_keys

    def get_cloumn(self, key_id):
        resp = requests.get(self.url_prefix + URLS.COLUMN_BY_SPLITKEY.format(key_id))
        if not resp.text:
            return None

        return [ColumnInfo(column['id'], column['splittingKeyId'], column['table'], column['column'])
                    for column in json.loads(resp.text)]

    def get_tblrule(self, key_id):
        resp = requests.get(self.url_prefix + URLS.RULES_BYSPLITKEY.format(key_id))
        if not resp.text:
            return None

        return [(int(tbl_rule['dbGroupId']), tbl_rule['tableSuffix'])
                    for tbl_rule in json.loads(resp.text)]

    def get_grpid2_tblsuf(self, cluster_name, table_name):
        db_cluster = self.get_cluster(cluster_name)
        split_keys = self.get_splitkey(db_cluster.id)

        key_ids = []
        for split_key in split_keys:
            columns = self.get_cloumn(split_key.id)
            key_ids.extend(column.splitKeyId for column in columns if column.table == table_name)

        return self.get_tblrule(key_ids[0]) if len(key_ids) != 0 else None


class URLS:
    # EVENT = 'http://localhost:8888/'
    CLUSTER = '/dbcluster/querysingle/{0}'
    DBGROUPBY_CLUSTER = '/dbgroup/{0}'
    DBINFO_GROUP_NAME = '/dbinfo/groupName/{0}'
    DBINFO_GROUP_ID = '/dbinfo/groupId/{0}'
    SPLITTINGKEY_BY_CLUSTER_ID = '/splitkey/getSplitKeyByClusterId/{0}'
    COLUMN_BY_SPLITKEY = '/splitkey/getColumsByKeyId/{0}'
    RULES_BYSPLITKEY = '/splitkey/ruleinfo/splitKeyId/{0}'
