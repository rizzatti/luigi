# -*- coding: utf-8 -*-

import luigi
from luigi.contrib.hdfs import abstract_client as hdfs_abstract_client
from luigi.contrib.hdfs import config as hdfs_config
from luigi.target import FileAlreadyExists


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
        self._conn = hive.connect(host=self._host, port=self._port, username=self._username, database=self._database)
        return self._conn

    def cursor(self):
        return self.connection().cursor()

    def exists(self, path):
        from pyhive.exc import OperationalError
        try:
            self.cursor().execute('dfs -stat {}'.format(path))
            return True
        except OperationalError:
            return False

    def move(self, source, destination):
        from pyhive.exc import OperationalError
        try:
            if not isinstance(source, (list, tuple)):
                source = [source]
            self.cursor().execute('dfs -mv {} {}'.format(' '.join(source), destination))
            return True
        except OperationalError:
            return False

    def rename(self, source, destination):
        return self.move(source, destination)

    def remove(self, path, recursrive=True, skip_trash=False):
        from pyhive.exc import OperationalError
        try:
            recursive_arg = '-r' if recursive else ''
            skip_arg = '-skipTrash' if skip_trash else ''
            self.cursor().execute('dfs -rm {} {} {}'.format(recursive_arg, skip_arg, path))
            return True
        except OperationalError:
            return False

    def chmod(self, path, permissions, recursive=False):
        from pyhive.exc import OperationalError
        try:
            recursive_arg = '-R' if recursive else ''
            self.cursor().execute('dfs -chmod {} {} {}'.format(recursive_arg, permissions, path))
            return True
        except OperationalError:
            return False

    def chown(self, path, owner, group, recursive=False):
        from pyhive.exc import OperationalError
        try:
            recursive_arg = '-R' if recursive else ''
            owner = '' if owner is None else owner
            group = '' if group is None else group
            ownership = '{}:{}'.format(owner, group)
            self.cursor().execute('dfs -chmod {} {} {}'.format(recursive_arg, ownership, path))
            return True
        except OperationalError:
            return False

    def count(self, path):
        raise NotImplementedError("HiveServer2 in luigi doesn't implement count")

    def copy(self, source, destination):
        from pyhive.exc import OperationalError
        try:
            self.cursor().execute('dfs -cp {} {}'.format(source, destination))
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

    def mkdir(self, path, parents=True, raise_if_exists=False):
        from pyhive.exc import OperationalError
        try:
            parent_arg = "-p" if parents else ""
            self.cursor().execute('dfs -mkdir {} {}'.format(parent_arg, path))
            return True
        except OperationalError as e:
            if raise_if_exists:
                raise FileAlreadyExists(e.stderr)
            return False

    def listdir(self, path, ignore_directories=False, ignore_files=False,
                include_size=False, include_type=False, include_time=False, recursive=False):
        raise NotImplementedError("HiveServer2 in luigi doesn't implement count")

    def touchz(self, path):
        from pyhive.exc import OperationalError
        try:
            self.cursor().execute('dfs -touchz {}'.format(path))
            return True
        except OperationalError:
            return False
