#!/usr/bin/env python
#encoding: utf-8

import unittest
from mysql import connector
from com.wangyin.cds.toolkit.rest.cds import CdsClient
from com.wangyin.cds.toolkit.util.clusters import ClusterUtil


class TblRuleInitTestCase(unittest.TestCase):
    ##初始化工作
    def setUp(self):
        self.cds = CdsClient('127.0.0.1', '8088', '8888')
        cluster_util = ClusterUtil()
        conn = connector.connect(user='zy', password='abcd1234', host='127.0.0.1', port=3306, database='')
        cluster_util.create_sample_app(conn)
        cluster_util.create_sample_cluster(conn)

    #退出清理工作
    def tearDown(self):
        pass

    #具体的测试用例，一定要以test开头
    def test_ruleinit(self):
        print self.cds.get_events()


if __name__ == '__main__':
    # tests = ['testGroupByClusterName']
    # 构造测试集
    # suite = unittest.TestSuite(map(RestClientTestCase, tests))
    # suite.addTest(RestClientTestCase('testGroupByClusterName'))
    suite = unittest.TestLoader().loadTestsFromTestCase(TblRuleInitTestCase)
    # suite = unittest.TestLoader().loadTestsFromModule('com.wangyin.cds.toolkit.testcase')
    # 执行测试
    runner = unittest.TextTestRunner()
    runner.run(suite)