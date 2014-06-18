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
        self.headers = {'Host': '192.168.1.101', 'Connection': 'keep-alive', 'Accept-Encoding': 'gzip', 'Poll-Req-Type': 'Req-Check-event'}

    def get_events(self):
        try:
            r = requests.get(self.event_url, headers=self.headers)
            self.headers['poll-session-id'] = r.headers['poll-session-id']
            events = json.loads(r.text)
            return events
        except RequestException as ex:
            print 'Fail to connect cds_server', ex
        else:
            return None

    def _url_format(self, template, **kwargs):
        return self.url_prefix + template.format(kwargs)

    def get_cluster(self, cluster_name):
        # URL = self.url_prefix + URLS.CLUSTER.format(cluster_name)
        r = requests.get(self._url_format(self, URLS.CLUSTER, cluster_name))
        if not r.text:
            return None
        cluster = json.loads(r.text)
        if len(cluster) != 0:
            cluster = DbCluster(cluster['id'], cluster['clusterName'])
            groups = self.get_group(cluster_name)
            for group in groups:
                print group.type
                if group.type == 'global':
                    cluster.set_global_group(group)
                if group.type == 'worker':
                    cluster.add_worker_group(group)
        print 'di', len(cluster)
        return cluster if len(cluster) == 0 else None

    def get_group(self, clusterName):
        URL = self.url_prefix + URLS.DBGROUPBY_CLUSTER.format(clusterName)
        r = requests.get(URL)
        groups = json.loads(r.text)
        retGroups = []
        if len(groups) != 0:
            for group in groups:
                group = DbGroup(group['id'], group['groupName'], group['groupType'])
                dbinfos = self.get_dbinfo(group.name)
                for dbinfo in dbinfos:
                    group.add_space(dbinfo, dbinfo.isMaster)
                retGroups.append(group)
        return retGroups

    def get_dbinfo(self, group_name):
        URL = self.url_prefix + URLS.DBINFO_GROUP_NAME.format(group_name)
        r = requests.get(URL)
        dbinfos = json.loads(r.text)
        retDbinfos = []
        if len(dbinfos) != 0:
            for dbinfo in dbinfos:
                dbinfo = DbInfo(dbinfo['id'], dbinfo['ip'], dbinfo['port'], dbinfo['dbName'], dbinfo['masterOrSlave'] == 'Master')
                retDbinfos.append(dbinfo)
        return retDbinfos

    def get_splitkey(self, cluster_id):
        URL = self.url_prefix + URLS.SPLITTINGKEY_BY_CLUSTER_ID.format(cluster_id)
        r = requests.get(URL)
        keys = json.loads(r.text)
        retKeys = []
        if len(keys) != 0:
            for key in keys:
                key = SplittingKey(key['id'], key['splitName'], key['clusterId'])
                retKeys.append(key)
        return retKeys

    def get_cloumn(self, key_id):
        URL = self.url_prefix + URLS.COLUMN_BY_SPLITKEY.format(key_id)
        r = requests.get(URL)
        columns = json.loads(r.text)
        retCols = []
        if len(columns) != 0:
            for column in columns:
                columnn = ColumnInfo(column['id'], column['splittingKeyId'], column['table'], column['column'])
                retCols.append(columnn)
        return retCols

    def get_tblrule(self, key_id):
        URL = self.url_prefix + URLS.RULES_BYSPLITKEY.format(key_id)
        r = requests.get(URL)
        depotRules = json.loads(r.text)
        retRules = []
        if len(depotRules) != 0:
            for depotRule in depotRules:
                retRules.append((int(depotRule['dbGroupId']), depotRule['tableSuffix']))
        return retRules

    def get_grp2tblsuf(self, clusterName, tableName):
        cluster = self.get_cluster(clusterName)
        spliKeys = self.get_splitkey(cluster.id)
        keyIds = []
        for spliKey in spliKeys:
            columns = self.get_cloumn(spliKey.id)
            keyIds.extend(self.find_splitkey(columns, tableName))
        if keyIds:
            gpId2Tbs = self.get_tblrule(keyIds[0])
            return gpId2Tbs
        # print gpId2Tbs
        return None

    def find_splitkey(self, columns, table):
        keys = []
        for col in columns:
            if col.table == table:
                keys.append(col.splitKeyId)
        return keys


class URLS:
    # EVENT = 'http://localhost:8888/'
    CLUSTER = '/dbcluster/querysingle/{0}'
    DBGROUPBY_CLUSTER = '/dbgroup/{0}'
    DBINFO_GROUP_NAME = '/dbinfo/groupName/{0}'
    DBINFO_GROUP_ID = '/dbinfo/groupId/{0}'
    SPLITTINGKEY_BY_CLUSTER_ID = '/splitkey/getSplitKeyByClusterId/{0}'
    COLUMN_BY_SPLITKEY = '/splitkey/getColumsByKeyId/{0}'
    RULES_BYSPLITKEY = '/splitkey/ruleinfo/splitKeyId/{0}'
