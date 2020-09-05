import argparse
import collections
import datetime
import hashlib
import logging
import os
import random
import shutil
import subprocess
import time

from . import postgres


DEFAULT_LOGLEVEL = logging.WARNING
LOG = logging.getLogger(__name__)


class Operation:
    def __init__(self, name, func, args=None, run_by_default=True):
        self.name = name
        self.func = func
        self.args = args
        self.run_by_default = run_by_default

    def asTuple(self):
        return (self.name, self.func, self.args, self.run_by_default)

    def __iter__(self):
        return self.asTuple().__iter__()

    def __getitem__(self, idx):
        return self.asTuple().__getitem__(idx)


class BasicApp:
    def __init__(self, app_name):
        self.app_name = app_name

        self.parser = argparse.ArgumentParser(description=app_name)
        ops = self.parser.add_argument_group('operations')
        ops.add_argument('--list', help='list operations', action='store_true')
        ops.add_argument('--run', help='run specific operations (by default run all)', nargs='*', metavar='OPERATION')
    
        self.parser.add_argument('-v', help='increases verbosity', action='count', default=0)
        self.parser.add_argument('-q', help='decreases verbosity', action='count', default=0)

    def setupLogging(self, file_path, level_increase):
        loglevel = max(logging.DEBUG, min(logging.CRITICAL, DEFAULT_LOGLEVEL - level_increase * 10))

        # setup formatter
        log_format = '[%(asctime)-15s %(levelname)s] %(name)s: %(message)s'
        fmt = logging.Formatter(log_format)

        ch = logging.StreamHandler()
        ch.setFormatter(fmt)
        ch.setLevel(loglevel)
        logging.getLogger().addHandler(ch)

        # setup a file handler
        if os.path.exists(file_path):
            os.rename(file_path, f'{file_path}.0')
        fh = logging.FileHandler(file_path, mode='w')
        fh.setFormatter(fmt)
        fh.setLevel(logging.INFO)
        logging.getLogger().addHandler(fh)

        logging.getLogger('').setLevel(logging.DEBUG)

    def prepare(self):
        self.args = self.parser.parse_args()
        self.setupLogging(f'{self.app_name}.log', self.args.v - self.args.q)

    def setRandomSeed(self, name):
        """
        Sets the random seed in the `random` package and returns the seed, which is a 32 bit unsigned int
        """
        m = hashlib.sha256()
        m.update(name.encode('utf8'))
        seed = int.from_bytes(m.digest(), byteorder='little', signed=False) & 0xffffffff
        random.seed(seed)
        return seed

    def run(self):
        self.prepare()

        ops = self.getOperations()
        if self.args.run is None:
            ops = [ step for step in ops if step.run_by_default ]
        else:
            ops = [ step for step in ops if step.name in self.args.run ]
            missing_ops = self.args.run if len(ops) == 0 else [ name for name in self.args.run if name not in next(zip(*ops)) ]
            if len(missing_ops) > 0:
                raise ValueError(f'operations not available: {", ".join(missing_ops)}')

        if self.args.list:
            print('\n'.join([(op.name if op.run_by_default else f'({op.name})') for op in ops]))
            return

        for op in ops:
            self.setRandomSeed(op.name)

            timestr = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print('{} Processing {} ...'.format(timestr, op.name))

            t1_start = time.perf_counter()
            t2_start = time.process_time()

            if op.args is None:
                op.func()
            elif isinstance(op.args, dict):
                op.func(**op.args)
            else:
                op.func(*op.args)

            t1_stop = time.perf_counter()
            t2_stop = time.process_time()
            print(f"Processing {op.name}: {round((t1_stop - t1_start) / 60, 1)} minutes elapsed; "
                  f"{round((t2_stop - t2_start) / 60, 1)} minutes CPU time")


class PostgresApp(BasicApp):
    def __init__(self, app_name, database_credentials, default_database_schema, default_resultdir):
        super().__init__(app_name)

        self.database_credentials = database_credentials
        self.pgcon = None

        self.parser.add_argument('--resultdir', help="where to store results", default=default_resultdir)
        self.parser.add_argument('--sql-schema', help="SQL schema used", default=default_database_schema)


    def prepare(self):
        super().prepare()
        self.resultdir = self.args.resultdir

    def setRandomSeed(self, name):
        seed = super().setRandomSeed(name)

        if self.pgcon is not None:
            with self.pgcon.cursor() as cur:
                cur.execute(f'SET seed TO {seed / 2**32}')

    def setupDatabase(self):
        assert self.database_credentials.host == 'localhost', 'database host is not localhost; please change host or use an existing database'

        sql = f""" \
            CREATE USER {self.database_credentials.user} WITH PASSWORD '{self.database_credentials.password}'; \
            CREATE DATABASE {self.database_credentials.database} WITH OWNER {self.database_credentials.user}; \
        """

        with subprocess.Popen(['sudo', '-u', 'postgres', 'psql'], stdin=subprocess.PIPE) as proc:
            proc.stdin.write(sql.encode('utf8'))

    def clearResults(self):
        if os.path.exists(self.resultdir):
            shutil.rmtree(self.resultdir)

        postgres.reset_schema(self.database_credentials, self.args.sql_schema, create_schema=False, drop_schema=True)

    def setupConnection(self):
        postgres.reset_schema(self.database_credentials, self.args.sql_schema, create_schema=True, drop_schema=False)
        self.pgcon = postgres.pgconnect(credentials=self.database_credentials, schema=self.args.sql_schema)

    def runStep(self, func, ctx):
        func()

    def close(self):
        if self.pgcon is not None:
            self.pgcon.close()
            self.pgcon = None


class StepFunction(collections.namedtuple('StepFunction', ['func', 'args'])):
    def __call__(self):
        if isinstance(self.args, dict):
            return self.func(**self.args)
        else:
            return self.func(*self.args)
