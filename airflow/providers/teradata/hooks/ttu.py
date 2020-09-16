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

import os
import uuid
import logging
import shlex

from airflow import configuration
from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException
from airflow.utils.file import TemporaryDirectory

from tempfile import gettempdir, NamedTemporaryFile
from builtins import bytes
from subprocess import Popen, STDOUT, PIPE

log = logging.getLogger(__name__)

class TtuHook(BaseHook):
    """
    Interact with Teradata using Teradata Tools and Utilities (TTU) binaries.
    Note: it is required that TTU previously installed and configured propertly.

    extras example: ``{"bteq_quit_zero":true, "bteq_session_encoding";"utf-8"}``
    """

    def __init__(self, conn_id):
        super().__init__()
        self.conn_id = conn_id
        self.conn = None

    def get_conn(self):
        if not self.conn:
            connection = self.get_connection(self.conn_id)
            extras = connection.extra_dejson
            self.conn = dict(
                username=connection.login,
                password=connection.password,
                ttu_output_encoding=extras.get('output_encoding', 'utf-8'),
                ttu_log_folder=extras.get('ttu_log_folder', configuration.get('core', 'BASE_LOG_FOLDER')),
                ttu_max_sessions=self.extras.get('ttu_max_sessions', 1),
                bteq_session_encoding=self.extras.get('bteq_session_encoding', 'ASCII'),
                bteq_output_width=extras.get('bteq_output_width', '65531'),
                bteq_quit_zero=extras.get('bteq_quit_zero', False),
                sp = None
                )
        return self.conn

    def execute_bteq(self, bteq, xcom_push_flag=False):
        """
        Executes BTEQ sentences using BTEQ binary.
        :param bteq: string of BTEQ sentences
        :param xcom_push_flag: Flag for pushing last line of BTEQ Log to XCom
        """
        log.info("Executing BTEQ sentences...")
        with TemporaryDirectory(prefix='airflowtmp_ttu_bteq') as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, prefix=str(uuid.uuid1()), mode='wb') as f:
                f.write(bytes(self._prepare_bteq_script(bteq,
                                                       self.conn.host,
                                                       self.conn.login,
                                                       self.conn.password,
                                                       self.conn.bteq_output_width,
                                                       self.bteq_session_encoding,
                                                       self.conn.bteq_quit_zero
                                                       ),'utf_8'))
                f.flush()
                fname = f.name
                log.debug("Temporary BTEQ "
                             "location :{0}".format(fname))
                f.seek(0)
                self.conn.sp = Popen(
                    ['bteq'], stdin=f,
                    stdout=PIPE, stderr=STDOUT,
                    cwd=tmp_dir,
                    preexec_fn=os.setsid)
                line = ''
                failure_line = 'unknown reasons. Please see full BTEQ Output for more details.'
                for line in iter(self.conn.sp.stdout.readline, b''):
                    line = line.decode(self.output_encoding).strip()
                    log.info(line)
                    if "Failure" in line:
                        #Just save the last failure
                        failure_line = line
                self.conn.sp.wait()
                log.info("BTEQ exited with "
                             "return code {0}".format(self.conn.sp.returncode))

                if self.conn.sp.returncode:
                    raise AirflowException("BTEQ exited with return code " + str(self.conn.sp.returncode) + ' because of ' +
                                           failure_line)
                if xcom_push_flag:
                    return line

    def execute_tdload(self, input_file, table, delimiter=';', working_database=None, encoding='UTF8', xcom_push_flag=False, raise_on_rows_error=False, raise_on_rows_duplicated=False):
        """
        Load a CSV file to Teradata Table (previously created) using tdload binary.
        Note: You need to strip header of the CSV. tdload only accepts rows, not header.
        :param input_file : file to load
        :param table : output table
        :param delimeter : separator of the file to load
        :param encoding : encoding of the file to load
        :param working_database : teradata working database to use for staging data
        :param xcom_push_flag: Flag for pushing last line of BTEQ Log to XCom
        :raise_on_rows_error: if true, raise an error when found error loading some rows.
        :raise_on_rows_duplicated= if true, raise an error when found duplicated rows.

        """
        fload_out_path = self.conn.ttu_log_folder + '/tdload/out'
        if not os.path.exists(fload_out_path):
            log.debug('Creating directory ' + fload_out_path)
            os.makedirs(fload_out_path)

        fload_checkpoint_path = self.conn.ttu_log_folder + '/tdload/checkpoint'
        if not os.path.exists(fload_checkpoint_path):
            log.debug('Creating directory ' + fload_checkpoint_path)
            os.makedirs(fload_checkpoint_path)
        log.info('Loading file ' + input_file + ' into table ' + table + '')
        self.conn.sp = Popen(self._prepare_tdload_command(
                            input_file,
                            self.conn.host,
                            self.conn.login,
                            self.conn.password,
                            encoding,
                            table,
                            delimiter,
                            fload_out_path,
                            fload_checkpoint_path,
                            self.conn.max_sessions,
                            working_database
                         ),
            stdout=PIPE, stderr=STDOUT,preexec_fn=os.setsid)

    def execute_tptexport(self, sql, output_file, delimiter = ';', encoding='UTF8', xcom_push_flag=False, double_quote_varchar=True):
        """
        Export a table from Teradata Table using tpt binary.
        Note: The exported CSV file does not contains header row
        :param file : file to load
        :param delimeter : separator of the file to load
        :param encoding : encoding of the file to load
        :param table : output table
        :param max_sessions : max sessions to use
        :param xcom_push_flag: flag for pushing last line of BTEQ Log to XCom
        :param double_quote_varchar: if true, replace quotes with escaping char for Teradata SQL in TPT
        """
        fexp_out_path = self.conn.ttu_log_folder + '/tbuild/logs'
        if not os.path.exists(fexp_out_path):
            log.info('Creating directory ' + fexp_out_path)
            os.makedirs(fexp_out_path)

        fexp_checkpoint_path = self.conn.ttu_log_folder + '/tbuild/checkpoint'
        if not os.path.exists(fexp_checkpoint_path):
            log.info('Creating directory ' + fexp_checkpoint_path)
            os.makedirs(fexp_checkpoint_path)
        if double_quote_varchar:
            sql = sql.replace("'", "''")
        log.info("""Exporting SQL '""" + sql + """' to file """ + output_file + """ using TPT Export""")

        with TemporaryDirectory(prefix='airflowtmp_ttu_tpt') as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, prefix=str(uuid.uuid4()), mode='wb') as f:
                f.write(bytes(self._prepare_tpt_export_script(
                                        sql,
                                        output_file,
                                        encoding,
                                        delimiter,
                                        self.conn.host,
                                        self.conn.login,
                                        self.conn.password,
                                        self.conn.max_sessions,
                                        ), 'utf_8'))
                f.flush()
                fname = f.name
                log.debug("Temporary TPT Template "
                             "location :{0}".format(fname))
                f.seek(0)
                self.conn.sp = Popen(
                    ['tbuild', '-f', fname, 'airflow' + '_tpt_'  + str(uuid.uuid4())],
                    stdout=PIPE, stderr=STDOUT,
                    preexec_fn=os.setsid)

                line = ''
                error_line = 'unknown reasons. Please see full TPT Output for more details.'
                for line in iter(self.conn.sp.stdout.readline, b''):
                    line = line.decode(self.output_encoding).strip()
                    log.info(line)
                    if "error" in line:
                        #Just save the last error
                        error_line = line
                self.conn.sp.wait()
                log.info("tbuild command exited with "
                             "return code {0}".format(self.conn.sp.returncode))
                if self.conn.sp.returncode:
                    raise AirflowException("TPT command exited with return code " + str(self.conn.sp.returncode) + ' because of ' +
                                           error_line)

            if xcom_push_flag:
                return line

    def on_kill(self):
        log.debug('Killing child process...')
        self.conn.sp.kill()

    @staticmethod
    def _prepare_bteq_script(bteq_string, host, login, password, bteq_output_width, bteq_session_encoding, bteq_quit_zero) -> str:
        """
        Prepare a BTEQ file with connection parameters for executing SQL Sentences with BTEQ syntax.
        :param bteq_string : bteq sentences to execute
        :param host : Teradata Host
        :param login : username for login
        :param password : password for login
        :param bteq_output_width : width of BTEQ output in console
        :param bteq_session_encoding : session encoding. See offical teradata docs for possible values
        :param bteq_quit_zero : if True, force a .QUIT 0 sentence at the end of the sentences (forcing return code = 0)
        """
        bteq_list = [".LOGON {}/{},{};".format(host, login, password)]
        bteq_list += [".SET WIDTH " + bteq_output_width + ";"]
        bteq_list += [".SET SESSION CHARSET '" + bteq_session_encoding + "';"]
        bteq_list += [bteq]
        if bteq_quit_zero:
            bteq_list += [".QUIT 0;"]
        bteq_list += [".LOGOFF;"]
        bteq_list += [".EXIT;"]
        return "\n".join(bteq_list)

    @staticmethod
    def _prepare_tdload_command(input_file, host, login, password, encoding, table, delimiter, log_path, checkpoint_path, max_sessions, working_database, job_name) -> str:
        """
        Prepare a tdload file with connection parameters for loading data from file
        :param input_file : bteq sentences to execute
        :param host : Teradata Host
        :param login : username for login
        :param password : password for login
        :param encoding : width of BTEQ output in console
        :param table : table name. See offical teradata docs for possible values
        :param delimiter : file separator. See offical teradata docs for possible values
        :param log_path : path for command output log
        :param checkpoint_path : path for command checkpoint.
        :param max_sessions : how many sessions we use for loading data
        :param working_database : database for staging data
        :param job_name : job name
        """
        tdload_command = ['tdload']
        tdload_command += ['-f'] + [input_file]
        tdload_command += ['-u'] + [login]
        tdload_command += ['-p'] + [password]
        tdload_command += ['-h'] + [host]
        tdload_command += ['-c'] + [encoding]
        tdload_command += ['-t'] + [table]
        tdload_command += ['-d'] + [delimiter]
        tdload_command += ['-L'] + [log_path]
        tdload_command += ['-r'] + [checkpoint_path]
        tdload_command += ['--TargetMaxSessions'] + [str(max_sessions)]
        if working_database:
            tdload_command += ['--TargetWorkingDatabase'] + [working_database]
        tdload_command += ['airflow_ttu_tdload_'  + job_name ] #Job Name
        return tdload_command

    @staticmethod
    def _prepare_tpt_export_script(sql, output_file, encoding, delimiter, host, login, password, max_sessions, job_name= 'airflow_export') -> str:
        """
        Prepare a tpt script file with connection parameters for exporting data to CSV
        :param sql : SQL sentence to export
        :param output_file : path to output file
        :encoding : encoding of exported CSV file (see teradata docs for possible value)
        :delimiter : Delimiter for exported CSV file
        :param host : Teradata Host
        :param login : username for login
        :param password : password for login
        :param max_sessions : how many sessions we use for loading data
        :param job_name : job name
        """
        return '''
            USING CHARACTER SET {encoding}
            DEFINE JOB {job_name}
            (
                    APPLY
                    TO OPERATOR
                    (
                            $FILE_WRITER()

                            ATTRIBUTES
                            (
                                    FileName = '{filename}',
                                    Format = 'DELIMITED',
                                    OpenMode = 'Write',
                                    IndicatorMode = 'N',
                                    TextDelimiter = '{delimiter}'
                            )
                    )
                    SELECT * FROM OPERATOR
                    (
                            $EXPORT()

                            ATTRIBUTES
                            (
                                    UserName = '{username}',
                                    UserPassword = '{password}',
                                    SelectStmt = '{sql}',
                                    TdpId = '{host}',
                        MaxSessions = {max_sessions}
                            )
                    );
            );
            '''.format(filename=output_file, encoding=encoding, delimiter=delimiter, username=login,
                       password=password, sql=sql, host=host, max_sessions=max_sessions, job_name = job_name)


