import contextlib

import psycopg2


DEFAULT_SEARCH_PATH = ['public', 'contrib']


def pgconnect(credentials, schema=None, use_wrapper=True):
    credentials = dict(credentials.items())
    search_path = [schema] + DEFAULT_SEARCH_PATH if schema is not None else DEFAULT_SEARCH_PATH
    credentials['options'] = f'-c search_path={",".join(search_path)}'
    con = psycopg2.connect(**credentials)

    if use_wrapper:
        con = Connection(con)

    return con


class Connection:
    """
        Database connection object wrapper
    """
    def __init__(self, con, autocommit=True):
        self._con = con
        self._autocommit = autocommit
        self._con.set_client_encoding('UTF8')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()

    def commit(self):
        self._con.commit()

    @contextlib.contextmanager
    def cursor(self, commit=None, **kwargs):
        yield self._con.cursor(**kwargs)

        if commit or (commit is None and self._autocommit):
            self.commit()

    def vacuum(self):
        self._con.set_session(autocommit=True)
        with self.cursor() as cur:
            cur.execute('VACUUM')
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
