# -*- coding: utf-8 -*-
__author__ = 'zy'


class DbCluster:
    def __init__(self, id, name):
        self.id = id
        self.name = name
        self.global_group = None
        self.work_groups = []

    def set_global_group(self, group):
        self.global_group = group

    def add_worker_group(self, group):
        self.work_groups.append(group)


class DbGroup:
    def __init__(self, id, name, dbtype):
        self.id = id
        self.name = name
        self.type = dbtype
        self.master = None
        self.slave = []

    def add_dbinfo(self, dbinfo, is_master):
        if is_master:
            if self.master is not None:
                raise "multiple masters for group %s" % self.name
            else:
                self.master = dbinfo
        else:
            self.slave.append(dbinfo)


class DbInfo:
    def __init__(self, id, ip, port, schema, is_master):
        self.id = id
        self.ip = ip
        self.port = port
        self.schema = schema
        self.is_master = is_master


class SplittingKey:
    def __init__(self, id, name, cluster_id):
        self.id = id
        self.name = name
        self.cluster_id = cluster_id


class ColumnInfo:
    def __init__(self, id, split_key_id, table, column):
        self.id = id
        self.split_key_id = split_key_id
        self.table = table
        self.column = column


class SplittingRule:
    def __init__(self, id, type, group, low_bound, up_bound, hash_index):
        self.id = id
        self.type = type
        self.group = group
        self.low_bound = low_bound
        self.up_bound = up_bound
        self.hash_index = hash_index