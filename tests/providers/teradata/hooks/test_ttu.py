#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


import unittest
from unittest import mock
from unittest.mock import patch

from airflow.models import Connection
from airflow.providers.teradata.hooks.ttu import TtuHook


class TestTtuHookConn(unittest.TestCase):
    def setUp(self):
        super().setUp()

        self.connection = Connection(login='login', password='password', host='host',)

        class UnitTestTttuHook(TtuHook):
            conn_name_attr = 'ttu_conn_id'

        self.db_hook = UnitTestTttuHook()
        self.db_hook.get_connection = mock.Mock()
        self.db_hook.get_connection.return_value = self.connection

    @patch('airflow.providers.teradata.hooks.ttu.get_conn')
    def test_get_conn(self, mock_connect):
        self.db_hook.get_conn()
        mock_connect.assert_called_once_with(
            host='host', user='login', password="password"
        )


class TestTtuHook(unittest.TestCase):
    def setUp(self):
        super().setUp()

        self.cur = mock.MagicMock()
        self.conn = mock.MagicMock()
        self.conn.cursor.return_value = self.cur
        conn = self.conn

        class UnitTestTtuHook(TtuHook):
            conn_name_attr = 'ttu_conn_id'

            def get_conn(self):
                return conn

        self.db_hook = UnitTestTtuHook()
    #execute_bteq(self.sql, self.xcom_push)
    @patch('airflow.providers.teradata.hooks.ttu.execute_bteq')
    def test_execute_bteq(self, mock_sql):
        sql = '''
            SELECT 1;
            .QUIT 0
        '''
        self.db_hook.execute_bteq(sql)
        test_execute_bteq.assert_called_once_with(sql)
