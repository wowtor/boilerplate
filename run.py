#!/usr/bin/env python3

import contextlib
from functools import partial
import logging
import os

import confidence

from boilerplate.app import PostgresApp, Operation


LOG = logging.getLogger(__name__)


def calculate_square_root(app, num):
    with app.pgcon.cursor() as cur:
        cur.execute('SELECT SQRT(%s)', (num,))
        output = cur.fetchone()[0]

    print(f'the square root of {num} is {output}')


def get_operations(app):
    return [
        Operation(
            "create_database",
            app.setup_database,
            run_by_default=False,
        ),
        Operation(
            "clean",
            app.clear_results,
        ),
        Operation(
            "calculate_square_root",
            partial(calculate_square_root, app, 16),
        ),
    ]


if __name__ == '__main__':
    cfg = confidence.load_name('project', 'local')

    app = PostgresApp('my_project', get_operations, cfg.database.credentials, cfg.database.schema, cfg.resultdir)
    app.run()
