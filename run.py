#!/usr/bin/env python3

import argparse
import datetime
import logging
import os
import shutil
import time

import confidence

from postgres import pgconnect


DEFAULT_LOGLEVEL = logging.WARNING
LOG = logging.getLogger(__file__)


def setupLogging(args):
    loglevel = max(logging.DEBUG, min(logging.CRITICAL, DEFAULT_LOGLEVEL + (args.q - args.v) * 10))

    # setup formatter
    log_format = '[%(asctime)-15s %(levelname)s] %(name)s: %(message)s'
    fmt = logging.Formatter(log_format)

    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    ch.setLevel(loglevel)
    logging.getLogger().addHandler(ch)

    # setup a file handler
    if os.path.exists('run.log'):
        os.rename('run.log', 'run.log.0')
    fh = logging.FileHandler('run.log', mode='w')
    fh.setFormatter(fmt)
    fh.setLevel(logging.INFO)
    logging.getLogger().addHandler(fh)

    logging.getLogger('').setLevel(logging.DEBUG)


def reset_schema(credentials, schema, create_schema=False, drop_schema=False):
    with pgconnect(credentials) as con:
        with con.cursor() as cur:
            if drop_schema:
                LOG.info('drop schema (if exists)')
                cur.execute('DROP SCHEMA IF EXISTS %s CASCADE' % schema)

            if create_schema:
                cur.execute('CREATE SCHEMA IF NOT EXISTS %s' % schema)


def run(con, args, cfg):
    steps = [
        ('step1', func, (con,)),
    ]

    for name, step, step_args in steps:
        if vars(args)[name] or args.all:
            try:
                timestr = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                print('{} Processing {} ...'.format(timestr, name))

                t1_start = time.perf_counter()
                t2_start = time.process_time()

                if isinstance(step_args, dict):
                    step(**step_args)
                else:
                    step(*step_args)
                con.commit()

                t1_stop = time.perf_counter()
                t2_stop = time.process_time()
                print("Processing {name}: {elapsed:.1f} minutes elapsed; {cpu:.1f} minutes CPU time".format(name=name, elapsed=(t1_stop-t1_start)/60, cpu=(t2_stop-t2_start)/60))

            except Exception as e:
                LOG.fatal('processing step {step} failed: {desc}'.format(step=name, desc=str(e)))
                raise


if __name__ == '__main__':
    cfg = confidence.load_name('project', 'local')

    parser = argparse.ArgumentParser(description='boilerplate')
    ops = parser.add_argument_group('operations')
    ops.add_argument('--all', action='store_true')
    ops.add_argument('--clean', help="delete previous results", action='store_true')

    parser.add_argument('--resultdir', help="where to store results", default=cfg.resultdir)
    parser.add_argument('--sql-schema', help="SQL schema used", default=cfg.database.schema)
    parser.add_argument('-v', help='increases verbosity', action='count', default=0)
    parser.add_argument('-q', help='decreases verbosity', action='count', default=0)
    args = parser.parse_args()

    setupLogging(args)

    if args.clean and os.path.exists(args.resultdir):
        shutil.rmtree(args.resultdir)
    reset_schema(cfg.database.credentials, cfg.database.schema, create_schema=True, drop_schema=args.clean)

    with pgconnect(credentials=cfg.database.credentials, schema=cfg.database.schema) as con:
        run(con, args, cfg)
