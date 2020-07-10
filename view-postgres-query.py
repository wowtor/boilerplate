#!/usr/bin/env python

import argparse

import confidence

from postgres import pgconnect


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
        cur.execute('SELECT schemaname, tablename FROM pg_catalog.pg_tables ORDER BY schemaname, tablename')
        tables = cur.fetchall()

    for i in range(len(tables)):
        with con.cursor() as cur:
            try:
                cur.execute('''SELECT pg_relation_size('"{}"."{}"')'''.format(*tables[i]));
                tables[i] = list(tables[i]) + [cur.fetchone()[0], None]
            except Exception as e:
                cur.execute("ROLLBACK")
                tables[i] = list(tables[i]) + [None, str(e)]

    print(tabulate(tables, headers=['schema', 'table', 'bytes', 'comment']))


def list_queries(con, killall=False):
    with con.cursor() as cur:
        cur.execute('''
            SELECT
                pid,
                now() - pg_stat_activity.query_start AS duration,
                query,
                state
            FROM pg_stat_activity
            WHERE state = 'active'
                AND (now() - pg_stat_activity.query_start) > interval '15 seconds'
            ORDER BY duration DESC
        ''')

        for pid, duration, query, state in cur.fetchall():
            if killall:
                kill(con, pid)
            else:
                print('query {} running for {}: {}'.format(pid, describe_interval(duration.total_seconds()), query))


if __name__ == '__main__':
    cfg = confidence.load_name('project', 'local')

    parser = argparse.ArgumentParser(description='View long running queries.')
    parser.add_argument('--tables', help="List tables.", action='store_true')
    parser.add_argument('--queries', help="list queries.", action='store_true')
    parser.add_argument('--killall', help="kill all long running queries.", action='store_true')
    parser.add_argument('--kill', metavar='PID', help="kill a long running query.", type=int)
    args = parser.parse_args()

    with pgconnect(cfg.database.credentials, statement_timeout=1000) as con:
        if args.kill:
            kill(con, args.kill)
        if args.queries:
            list_queries(con)
        if args.killall:
            list_queries(con, killall=True)
        if args.tables:
            list_tables(con)
