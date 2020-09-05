#!/usr/bin/env python3

import contextlib
import logging

import confidence

from boilerplate.app import PostgresApp


LOG = logging.getLogger(__name__)


class ProjectApp(PostgresApp):
    def __init__(self, database_credentials, default_database_schema, default_resultdir):
        super().__init__('my_project', database_credentials, default_database_schema, default_resultdir)

    def doStuff(self):
        pass

    def getOperations(self):
        return [
            ('clean', self.clearResults),
            ('init', self.setupConnection),
            ('do_stuff', self.doStuff),
        ]


if __name__ == '__main__':
    cfg = confidence.load_name('my_project', 'local')

    with contextlib.closing(ProjectApp(cfg.database.credentials, cfg.database.schema, cfg.resultdir)) as app:
        app.run()
