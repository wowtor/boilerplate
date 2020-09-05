#!/usr/bin/env python

import argparse

import confidence
from tabulate import tabulate

from boilerplate.postgres import pgconnect


DEFAULT_MINIMUM_AGE = 15


def describe_interval(d):
    if d < 3*60:
        return '{:.1f} seconds'.format(d)
    elif d < 3*3600:
        return '{:.1f} minutes'.format(d/60)
    else:
        return '{:.1f} hours'.format(d/3600)


def kill(con, pid):
    with con.cursor() as cur:
        cur.execute('SELECT pg_cancel_backend(%s)', (pid,))
        print(f'killing {pid}: {"success" if cur.fetchone()[0] else "failed"}')


def list_tables(con):
    with con.cursor() as cur:
        cur.execute('''
                WITH entry as (
                    SELECT schemaname, tablename,
                        '"' || schemaname || '"."' || tablename || '"' as spec
                    FROM pg_catalog.pg_tables
                    WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
                ), tabstats as (
                    SELECT schemaname, tablename,
                        pg_table_size(spec) table_size,
                        pg_total_relation_size(spec) total_size
                    FROM entry
                )
                SELECT schemaname, tablename,
                    pg_size_pretty(table_size) table_size_pretty,
                    pg_size_pretty(total_size) total_size_pretty,
                    (100 * total_size / (SELECT SUM(total_size) FROM tabstats sumstats WHERE schemaname = tabstats.schemaname))::INT schema_perc
                FROM tabstats
                ORDER BY schemaname, total_size DESC
            ''');

        print(tabulate(cur.fetchall(), headers=['schema', 'table', 'table size', 'total size', 'schema %'], colalign=('left', 'left', 'right', 'right', 'right')))


def list_queries(con, age=0, killall=False):
    with con.cursor() as cur:
        cur.execute(f'''
            SELECT
                pid,
                now() - query_start AS duration,
                query,
                datname,
                client_addr,
                pg_blocking_pids(pid) blocked_by
            FROM pg_stat_activity
            WHERE state = 'active'
                AND (now() - query_start) > interval '{age} seconds'
            ORDER BY duration DESC
        ''')

        for pid, duration, query, datname, client_addr, blocked_by in cur.fetchall():
            if killall:
                kill(con, pid)
            else:
                blocked_str = '' if not len(blocked_by) else '; blocked by ' + ', '.join([str(p) for p in blocked_by])
                print(f'[{pid}] running on {datname} from {client_addr} for {describe_interval(duration.total_seconds())}{blocked_str}: {query}')


if __name__ == '__main__':
    cfg = confidence.load_name('my_project', 'local')

    parser = argparse.ArgumentParser(description='View long running queries.')
    parser.add_argument('--tables', help="List tables.", action='store_true')
    parser.add_argument('--queries', help="list queries.", action='store_true')
    parser.add_argument('--minimum-age', metavar='SECONDS', default=15, type=int, help=f"minimum age of listed queries (default: {DEFAULT_MINIMUM_AGE}).")
    parser.add_argument('--killall', help="kill all listed queries.", action='store_true')
    parser.add_argument('--kill', metavar='PID', help="kill a long running query.", type=int)
    parser.add_argument('--vacuum', help="run vacuum.", action='store_true')
    parser.add_argument('--vacuum-full', help="run vacuum full.", action='store_true')
    args = parser.parse_args()

    with pgconnect(cfg.database.credentials, statement_timeout=1000) as con:
        if args.kill:
            kill(con, args.kill)

        if args.queries:
            list_queries(con, age=args.minimum_age)
        if args.killall:
            list_queries(con, age=args.minimum_age, killall=True)
        if args.tables:
            list_tables(con)

    if args.vacuum or args.vacuum_full:
        with pgconnect(cfg.database.credentials) as con:
            con.vacuum(full=args.vacuum_full)
