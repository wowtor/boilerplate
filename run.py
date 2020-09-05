#!/usr/bin/env python3

import contextlib
import logging
import os

import confidence

from boilerplate.app import PostgresApp, Operation


LOG = logging.getLogger(__name__)


class ProjectApp(PostgresApp):
    def __init__(self, database_credentials, default_database_schema, default_resultdir):
        super().__init__('my_project', database_credentials, default_database_schema, default_resultdir)
        self.parser.add_argument('--setup-database', help='create user and database on local postgres server', action='store_true')

    def doStuff(self):
        pass

    def getOperations(self):
        return [
            Operation('create_database', self.setupDatabase, run_by_default=False),
            Operation('clean', self.clearResults),
            Operation('init', self.setupConnection),
            Operation('do_stuff', self.doStuff),
        ]


if __name__ == '__main__':
    cfg = confidence.load_name('project', 'local')

    with contextlib.closing(ProjectApp(cfg.database.credentials, cfg.database.schema, cfg.resultdir)) as app:
        app.run()
