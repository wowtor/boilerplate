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
    """
    Container of ETL descriptions.
    """

    def __init__(self, name: str, func, run_by_default: bool = False):
        """
        Creates an ETL metadata object.

        Examples of use:
            ```python
            ops = [
                    Operation("my_operation", my_function),
                    Operation("another_operation", functools.partial(function_which_requires_arguments, first_arg, second_arg))
                ]
            ```

        parameters:
            - name (str): the unique name of this operation
            - func (callable): the function to call in order to execute this operation
            - run_by_default (boolean): whether to run this operation if no arguments are provided
        """
        self.name = name
        self.func = func
        self.run_by_default = run_by_default

    def as_tuple(self):
        return (self.name, self.func, self.run_by_default)

    def __iter__(self):
        return self.as_tuple().__iter__()

    def __getitem__(self, idx):
        return self.as_tuple().__getitem__(idx)


class BasicApp:
    def __init__(self, app_name: str):
        self.app_name = app_name

        self.parser = argparse.ArgumentParser(description=app_name)
        ops = self.parser.add_argument_group("operations")
        ops.add_argument("--list", help="list operations", action="store_true")
        ops.add_argument(
            "--run",
            help="run specific operations (by default run all)",
            nargs="*",
            metavar="OPERATION",
        )

        self.parser.add_argument(
            "-v", help="increases verbosity", action="count", default=0
        )
        self.parser.add_argument(
            "-q", help="decreases verbosity", action="count", default=0
        )

    def setup_logging(self, file_path: str, level_increase: int):
        loglevel = max(
            logging.DEBUG, min(logging.CRITICAL, DEFAULT_LOGLEVEL - level_increase * 10)
        )

        # setup formatter
        log_format = "[%(asctime)-15s %(levelname)s] %(name)s: %(message)s"
        fmt = logging.Formatter(log_format)

        ch = logging.StreamHandler()
        ch.setFormatter(fmt)
        ch.setLevel(loglevel)
        logging.getLogger().addHandler(ch)

        # setup a file handler
        if os.path.exists(file_path):
            os.rename(file_path, f"{file_path}.0")
        fh = logging.FileHandler(file_path, mode="w")
        fh.setFormatter(fmt)
        fh.setLevel(logging.INFO)
        logging.getLogger().addHandler(fh)

        logging.getLogger("").setLevel(logging.DEBUG)

    def prepare(self):
        """
        This is called before the application is run.
        This parses the required args on start and sets up logging
        """

        self.args = self.parser.parse_args()
        self.setup_logging(f"{self.app_name}.log", self.args.v - self.args.q)

    def set_random_seed(self, name: str):
        """
        Sets the random seed in the `random` package and returns the seed, which is a 32 bit unsigned int.

        The seed is derived deterministically from `name`.
        """
        m = hashlib.sha256()
        m.update(name.encode("utf8"))
        seed = int.from_bytes(m.digest(), byteorder="little", signed=False) & 0xFFFFFFFF
        random.seed(seed)
        return seed

    def get_operations(self):
        raise NotImplementedError('should be implemented by subclass')

    def run(self):
        """
        Run all or some operations in this application.
        """
        # execute before run hook
        self.prepare()

        ops = self.get_operations()
        if self.args.run is None:
            ops = [step for step in ops if step.run_by_default]
        else:
            ops = [step for step in ops if step.name in self.args.run]
            missing_ops = (
                self.args.run
                if len(ops) == 0
                else [name for name in self.args.run if name not in next(zip(*ops))]
            )
            if len(missing_ops) > 0:
                raise ValueError(f'operations not available: {", ".join(missing_ops)}')

        if self.args.list:
            print(
                "\n".join(
                    [(op.name if op.run_by_default else f"({op.name})") for op in ops]
                )
            )
            return

        for op in ops:
            # TODO: This line does not do anything. It calls a function which returns something which is not used
            self.set_random_seed(op.name)

            timestr = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print("{} Processing {} ...".format(timestr, op.name))

            t1_start = time.perf_counter()
            t2_start = time.process_time()

            op.func()

            t1_stop = time.perf_counter()
            t2_stop = time.process_time()
            print(
                f"Processing {op.name}: {round((t1_stop - t1_start) / 60, 1)} minutes elapsed; "
                f"{round((t2_stop - t2_start) / 60, 1)} minutes CPU time"
            )


class PostgresApp(BasicApp):
    def __init__(
        self, app_name: str, database_credentials: dict, default_database_schema: str, default_resultdir: str
    ):
        """
        Subclass of BasicApp which adds Postgres connection management.
        """
        super().__init__(app_name)

        self.database_credentials = database_credentials
        self._pgcon = None
        self._pgseed = None

        self.parser.add_argument(
            "--resultdir", help="where to store results", default=default_resultdir
        )
        self.parser.add_argument(
            "--sql-schema", help="SQL schema used", default=default_database_schema
        )

    def prepare(self):
        super().prepare()
        self.resultdir = self.args.resultdir

    def set_random_seed(self, name: str):
        seed = super().set_random_seed(name)
        self.set_postgres_seed(seed)
        return seed

    def set_postgres_seed(self, seed):
        if self._pgcon is None:
            self._pgseed = seed  # no connection yet; postpone setting seed
            return

        with self._pgcon.cursor() as cur:
            cur.execute(f"SET seed TO {seed / 2**32}")

    def setup_database(self):
        """
        Initializes a database; this only works if a local postgres server is
        used. Creates a database and a user which has the required permissions.

        Alternatively, setup a database manually.
        """
        assert (
            self.database_credentials.host == "localhost"
        ), "database host is not localhost; please change host or use an existing database"

        # This creates a new database and installs
        sql = f""" \
            CREATE USER {self.database_credentials.user} WITH PASSWORD '{self.database_credentials.password}'; \
            CREATE DATABASE {self.database_credentials.database} WITH OWNER {self.database_credentials.user}; \
        """

        # this will not work when your database is on a different machine
        with subprocess.Popen(
            ["sudo", "-u", "postgres", "psql"], stdin=subprocess.PIPE
        ) as proc:
            proc.stdin.write(sql.encode("utf8"))

    def clear_results(self):
        """
        Deletes all results; deletes both `resultdir` and the database schema.
        """
        if os.path.exists(self.resultdir):
            shutil.rmtree(self.resultdir)

        postgres.reset_schema(
            self.database_credentials,
            self.args.sql_schema,
            create_schema=False,
            drop_schema=True,
        )

    def setup_connection(self):
        """
        Creates a connection to the database.
        """
        postgres.reset_schema(
            self.database_credentials,
            self.args.sql_schema,
            create_schema=True,
            drop_schema=False,
        )

        self._pgcon = postgres.pgconnect(
            credentials=self.database_credentials,
            schema=self.args.sql_schema,
            use_wrapper=False,
        )

        if self._pgseed is not None:  # seed setting postponed; do it now
            self.set_postgres_seed(self._pgseed)

    @property
    def pgcon(self):
        """
        Database connection object; creates connection when needed
        """
        if self._pgcon is None:
            self.setup_connection()

        return self._pgcon

    def run_step(self, func, ctx):
        func()

    def close(self):
        if self._pgcon is not None:
            self._pgcon.close()
            self._pgcon = None


class StepFunction(collections.namedtuple("StepFunction", ["func", "args"])):
    def __call__(self):
        if isinstance(self.args, dict):
            return self.func(**self.args)
        else:
            return self.func(*self.args)
