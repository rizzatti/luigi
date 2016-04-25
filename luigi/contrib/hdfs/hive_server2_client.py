# -*- coding: utf-8 -*-

import luigi
from luigi.contrib.hdfs import abstract_client as hdfs_abstract_client
from luigi.contrib.hdfs import config as hdfs_config


class hiveserver2(luigi.Config):
    port = luigi.IntParameter(default=10000)
    host = luigi.Parameter(default=None, config_path=dict(section='hdfs', name='namenode_host'))
    username = luigi.Parameter(default=None, config_path=dict(section='hdfs', name='user'))
    database = luigi.Parameter(default='default', config_path=dict(section='hdfs', name='database'))


class HiveServer2HdfsClient(hdfs_abstract_client.HdfsFileSystem):

    def __init__(self, host=None, port=None, user=None, database=None):
        self._host = host or hiveserver2().host
        self._port = port or hiveserver2().port
        self._username = user or hiveserver2().username
        self._database = database or hiveserver2().database

    def connection(self):
        if hasattr(self, '_conn'):
            return self._conn
        from pyhive import hive
        self.conn = hive.connect(host=self._host, port=self._port, username=self._username, database=self._database)
        return self._conn

    def cursor(self):
        return self.connection().cursor()

    def exists(self, path):
        from pyhive.exc import OperationalError
        try:
            self.cursor().execute('dfs -stat -e {}'.format(path))
            return True
        except OperationalError:
            return False

    def put(self, source, destination):
        from pyhive.exc import OperationalError
        try:
            self.cursor().execute('dfs -put {} {}'.format(source, destination))
            return True
        except OperationalError:
            return False

    def get(self, source, destination):
        from pyhive.exc import OperationalError
        try:
            self.cursor().execute('dfs -get {} {}'.format(source, destination))
            return True
        except OperationalError:
            return False

    def mkdir(self, path, parents=True):
        from pyhive.exc import OperationalError
        try:
            parent_arg = "-p" if parents else ""
            self.cursor().execute('dfs -mkdir {} {}'.format(parent_arg, path))
            return True
        except OperationalError:
            return False
