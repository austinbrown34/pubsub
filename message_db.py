import config.settings as settings
import psycopg2
from psycopg2.extras import RealDictCursor, Json
from psycopg2.extensions import register_adapter
from sql import sql_helper as sql

register_adapter(dict, Json)


class MessageDBService:
    def __init__(self, host=None, port=None, database=None,
                username=None, password=None, timeout=None):
        self.host = host if host else settings.MESSAGE_DB_HOST
        self.port = port if port else settings.MESSAGE_DB_PORT
        self.database = database if database else settings.MESSAGE_DB_DATABASE
        self.username = username if username else settings.MESSAGE_DB_USERNAME
        self.password = password if password else settings.MESSAGE_DB_PASSWORD
        self.timeout = timeout if timeout else settings.MESSAGE_DB_TIMEOUT
        self.connection = psycopg2.connect(
            host=self.host,
            database=self.database,
            user=self.username,
            password=self.password,
            port=self.port,
            options=f'-c statement_timeout={self.timeout * 1000}'
        )

    def execute_and_fetchall(self, sqlstring, params=None):
        """ get a cursor, execute the sql, return results"""
        params = params or ()
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sqlstring, params)
                return cur.fetchall()
        except Exception as e:
            print(str(e))
            self.connection.rollback()
            return None

    def execute_and_fetchone(self, sqlstring, params=None):
        """ get a cursor, execute the sql, return results"""
        params = params or ()
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sqlstring, params)
                return cur.fetchone()
        except Exception as e:
            print(str(e))
            self.connection.rollback()
            return None

    def write_message(self, id, stream_name, type, data,
                    metadata=None, expected_version=None):
        record = sql.execute_sql_file(
            self,
            'write_message.sql',
            {
                'id': id,
                'stream_name': stream_name,
                'type': type,
                'data': Json(data),
                'metadata': Json(metadata),
                'expected_version': expected_version
            },
            fetchall=False
        )
        self.connection.commit()
        return record

    def get_stream_messages(self, stream_name, position=0,
                            batch_size=1000, condition=None):
        records = sql.execute_sql_file(
            self,
            'get_stream_messages.sql',
            {
                'stream_name': stream_name,
                'position': position,
                'batch_size': batch_size,
                'condition': condition
            },
            fetchall=True
        )
        return records

    def get_category_messages(self, category_name, position=0, batch_size=1000,
                            correlation=None, consumer_group_member=None,
                            consumer_group_size=None, condition=None):
        records = sql.execute_sql_file(
            self,
            'get_category_messages.sql',
            {
                'category_name': category_name,
                'position': position,
                'batch_size': batch_size,
                'correlation': correlation,
                'consumer_group_member': consumer_group_member,
                'consumer_group_size': consumer_group_size,
                'condition': condition
            },
            fetchall=True
        )
        return records

    def get_last_stream_message(self, stream_name):
        record = sql.execute_sql_file(
            self,
            'get_last_stream_message.sql',
            {
                'stream_name': stream_name
            },
            fetchall=False
        )
        return record

    def stream_version(self, stream_name):
        record = sql.execute_sql_file(
            self,
            'stream_version.sql',
            {
                'stream_name': stream_name
            },
            fetchall=False
        )
        return record

    def id(self, stream_name):
        record = sql.execute_sql_file(
            self,
            'id.sql',
            {
                'stream_name': stream_name
            },
            fetchall=False
        )
        return record

    def cardinal_id(self, stream_name):
        record = sql.execute_sql_file(
            self,
            'cardinal_id.sql',
            {
                'stream_name': stream_name
            },
            fetchall=False
        )
        return record

    def category(self, stream_name):
        record = sql.execute_sql_file(
            self,
            'category.sql',
            {
                'stream_name': stream_name
            },
            fetchall=False
        )
        return record

    def is_category(self, stream_name):
        record = sql.execute_sql_file(
            self,
            'is_category.sql',
            {
                'stream_name': stream_name
            },
            fetchall=False
        )
        return record

    def acquire_lock(self, stream_name):
        record = sql.execute_sql_file(
            self,
            'acquire_lock.sql',
            {
                'stream_name': stream_name
            },
            fetchall=False
        )
        return record

    def hash_64(self, value):
        record = sql.execute_sql_file(
            self,
            'hash_64.sql',
            {
                'value': value
            },
            fetchall=False
        )
        return record

    def message_store_version(self):
        record = sql.execute_sql_file(
            self,
            'message_store_version.sql',
            {},
            fetchall=False
        )
        return record

    def clear_messages(self):
        records = sql.execute_sql_file(
            self,
            'clear_messages.sql',
            {},
            fetchall=True
        )
        return records

    def print_category_type_summary(self, category=None):
        sql_file = 'print_category_type_summary_all.sql'
        params = {}
        if category:
            sql_file = 'print_category_type_summary_specific.sql'
            params = {
                'category': category
            }
        print(
            sql.execute_sql_file(
                self,
                sql_file.replace('.sql', '_count.sql'),
                params,
                fetchall=True
            )
        )
        records = sql.execute_sql_file(
            self,
            sql_file,
            params,
            fetchall=True
        )
        return records

    def print_messages(self, stream_name=None):
        sql_file = 'print_messages_all.sql'
        params = {}
        if stream_name:
            sql_file = 'print_messages_specific.sql'
            params = {
                'stream_name': stream_name
            }
        records = sql.execute_sql_file(
            self,
            sql_file,
            params,
            fetchall=True
        )
        return records

    def print_stream_summary(self, stream_name=None):
        sql_file = 'print_stream_summary_all.sql'
        params = {}
        if stream_name:
            sql_file = 'print_stream_summary_specific.sql'
            params = {
                'stream_name': stream_name
            }
        print(
            sql.execute_sql_file(
                self,
                sql_file.replace('.sql', '_count.sql'),
                params,
                fetchall=True
            )
        )
        records = sql.execute_sql_file(
            self,
            sql_file,
            params,
            fetchall=True
        )
        return records

    def print_stream_type_summary(self, stream_name=None):
        sql_file = 'print_stream_type_summary_all.sql'
        params = {}
        if stream_name:
            sql_file = 'print_stream_type_summary_specific.sql'
            params = {
                'stream_name': stream_name
            }
        print(
            sql.execute_sql_file(
                self,
                sql_file.replace('.sql', '_count.sql'),
                params,
                fetchall=True
            )
        )
        records = sql.execute_sql_file(
            self,
            sql_file,
            params,
            fetchall=True
        )
        return records

    def print_type_category_summary(self, type=None):
        sql_file = 'print_type_category_summary_all.sql'
        params = {}
        if type:
            sql_file = 'print_type_category_summary_specific.sql'
            params = {
                'type': type
            }
        print(
            sql.execute_sql_file(
                self,
                sql_file.replace('.sql', '_count.sql'),
                params,
                fetchall=True
            )
        )
        records = sql.execute_sql_file(
            self,
            sql_file,
            params,
            fetchall=True
        )
        return records

    def print_type_stream_summary(self, type=None):
        sql_file = 'print_type_stream_summary_all.sql'
        params = {}
        if type:
            sql_file = 'print_type_stream_summary_specific.sql'
            params = {
                'type': type
            }
        print(
            sql.execute_sql_file(
                self,
                sql_file.replace('.sql', '_count.sql'),
                params,
                fetchall=True
            )
        )
        records = sql.execute_sql_file(
            self,
            sql_file,
            params,
            fetchall=True
        )
        return records

    def print_type_summary(self, type=None):
        sql_file = 'print_type_summary_all.sql'
        params = {}
        if type:
            sql_file = 'print_type_summary_specific.sql'
            params = {
                'type': type
            }
        print(
            sql.execute_sql_file(
                self,
                sql_file.replace('.sql', '_count.sql'),
                params,
                fetchall=True
            )
        )
        records = sql.execute_sql_file(
            self,
            sql_file,
            params,
            fetchall=True
        )
        return records
