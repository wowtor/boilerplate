#!/usr/bin/env python

import argparse

import confidence

from schotresten.database.db_connection import DBConnection


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


if __name__ == '__main__':
    cfg = confidence.load_name('schotresten', 'local')

    parser = argparse.ArgumentParser(description='View long running queries.')
    parser.add_argument('--killall', help="kill all long running queries.", action='store_true')
    parser.add_argument('--kill', metavar='PID', help="kill a long running query.", type=int)
    args = parser.parse_args()

    view = not (args.killall or args.kill)

    con = DBConnection(**cfg.database.schotresten)
    if args.kill:
        kill(con, args.kill)

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
            if view:
                print('query {} running for {}: {}'.format(pid, describe_interval(duration.total_seconds()), query))
            if args.killall:
                kill(con, pid)
