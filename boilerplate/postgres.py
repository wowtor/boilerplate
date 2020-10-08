import logging

import psycopg2

DEFAULT_SEARCH_PATH = ['public', 'contrib']
LOG = logging.getLogger(__name__)


def pgconnect(credentials, schema=None, use_wrapper=True, statement_timeout=None, autocommit=False):
    credentials = dict(credentials.items())
    search_path = [schema] + DEFAULT_SEARCH_PATH if schema is not None else DEFAULT_SEARCH_PATH
    credentials['options'] = f'-c search_path={",".join(search_path)}'
    if statement_timeout is not None:
        credentials['options'] += f' -c statement_timeout={statement_timeout}'
    con = psycopg2.connect(**credentials)

    assert use_wrapper or not autocommit

    if use_wrapper:
        con = Connection(con, autocommit=autocommit)

    return con


def reset_schema(credentials, schema, create_schema=False, drop_schema=False):
    with pgconnect(credentials, autocommit=True) as con:
        with con.cursor() as cur:
            if drop_schema:
                LOG.info('drop schema (if exists)')
                cur.execute('DROP SCHEMA IF EXISTS %s CASCADE' % schema)

            if create_schema:
                cur.execute('CREATE SCHEMA IF NOT EXISTS %s' % schema)


class Cursor:
    def __init__(self, con, commit_on_close, **kw):
        self.connection = con
        self.commit_on_close = commit_on_close
        self._cur = self.connection.cursor(**kw)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()

    def __iter__(self):
        return self._cur.__iter__()

    def __next__(self):
        return self._cur.__next__()

    def execute(self, query, *args):
        try:
            self._cur.execute(query, *args)
            LOG.debug(f'query: {self._cur.query.decode("utf8")}')
        except Exception as e:
            LOG.warning(f'query failed: {self._cur.query.decode("utf8")}; error: {e}')
            raise

    def __getattr__(self, name):
        return eval(f'self._cur.{name}')

    def close(self):
        if self.commit_on_close:
            self.connection.commit()
        self._cur.close()


class Connection:
    """
        Database connection object wrapper
    """
    def __init__(self, con, autocommit=False):
        self._con = con
        self._autocommit = autocommit
        self._con.set_client_encoding('UTF8')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()

    def commit(self):
        self._con.commit()

    def cursor(self, commit=None, **kwargs):
        return Cursor(self._con, commit or (commit is None and self._autocommit), **kwargs)

    def vacuum(self, full=False, tables=None):
        """
        From the postgres manual:
        ```
        VACUUM [ ( option [, ...] ) ] [ table_and_columns [, ...] ]
        VACUUM [ FULL ] [ FREEZE ] [ VERBOSE ] [ ANALYZE ] [ table_and_columns [, ...] ]

        where option can be one of:

        FULL [ boolean ]
        FREEZE [ boolean ]
        VERBOSE [ boolean ]
        ANALYZE [ boolean ]
        DISABLE_PAGE_SKIPPING [ boolean ]
        SKIP_LOCKED [ boolean ]
        INDEX_CLEANUP [ boolean ]
        TRUNCATE [ boolean ]

        and table_and_columns is:

        table_name [ ( column_name [, ...] ) ]
        ```
        """
        q = ['VACUUM']
        if full:
            q.append('FULL')
        if tables is not None:
            q.append(','.join([f'"{tab}"' for tab in tables]))

        self._con.set_session(autocommit=True)
        with self.cursor() as cur:
            cur.execute(' '.join(q))
        self._con.set_session(autocommit=False)

    def close(self):
        if self._autocommit:
            self.commit()
        self._con.close()
        self._con = None

    def create_engine(self):
        import sqlalchemy
        return sqlalchemy.create_engine('postgresql://', creator=lambda: self._con)

    def __getattr__(self, name):
        return eval(f'self._con.{name}')
