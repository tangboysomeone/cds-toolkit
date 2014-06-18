#!/usr/bin/env python
#encoding: utf-8

import unittest
from com.wangyin.cds.toolkit.rest.cds import CdsClient
from com.wangyin.cds.toolkit.rest.model import *


class CdsClientTestCase(unittest.TestCase):
    ##初始化工作
    def setUp(self):
        self.cds = CdsClient('127.0.0.1', '8088', '8888')

    #退出清理工作
    def tearDown(self):
        pass

    #具体的测试用例，一定要以test开头
    def test_events(self):
        print self.cds.get_events()

    def test_get_group(self):
        db_groups = self.cds.get_group('sample')
        self.assertGreater(len(db_groups), 0, R"can't get any group")

    def test_get_dbinfo(self):
        db_infos = self.cds.get_dbinfo('test')
        self.assertGreater(len(db_infos), 0, 'dbInfos is empty')
        self.assertIsInstance(db_infos[0], DbSpace, 'dbinfo is not exist')

    def test_get_cluster(self):
        db_cluster = self.cds.get_cluster('sample')
        self.assertIsNotNone(db_cluster, r"dbCluster dosen't exist")

    def test_get_splitKey(self):
        split_key = self.cds.get_splitkey(4)
        self.assertIsNotNone(split_key, r"splitKey dosen't exist")

    def test_get_column(self):
        columns = self.cds.get_cloumn(4)
        self.assertGreater(len(columns), 0, r"there's no columns under splitKey")

    def test_get_tblrule(self):
        tbl_rules = self.cds.get_tblrule(4)
        self.assertGreater(len(tbl_rules), 0, r"there's no any tableRules under the splitKey")

    def test_get_grpid2_tblsuf(self):
        grp2_tbl_suffix = self.cds.get_grpid2_tblsuf('sample', 'MALL_USERS')
        self.assertGreater(len(grp2_tbl_suffix), 0, r"there's no any tablesuffix with group")


if __name__ == '__main__':
    # tests = ['testGroupByClusterName']
    # 构造测试集
    # suite = unittest.TestSuite(map(RestClientTestCase, tests))
    # suite.addTest(RestClientTestCase('testGroupByClusterName'))
    suite = unittest.TestLoader().loadTestsFromTestCase(CdsClientTestCase)
    # suite = unittest.TestLoader().loadTestsFromModule('com.wangyin.cds.toolkit.testcase')
    # 执行测试
    runner = unittest.TextTestRunner()
    runner.run(suite)