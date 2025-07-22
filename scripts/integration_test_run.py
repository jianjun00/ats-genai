#!/usr/bin/env python3
import os
import sys
import glob
import subprocess
from datetime import datetime

SNAPSHOT_DIR = os.path.join(os.path.dirname(__file__), '..', 'db', 'snapshot', 'intg')
DB_NAME = 'intg_trading_db'
DB_USER = os.environ.get('PGUSER', 'postgres')
DB_HOST = os.environ.get('PGHOST', 'localhost')
DB_PORT = os.environ.get('PGPORT', '5432')


def find_latest_snapshot():
    if not os.path.isdir(SNAPSHOT_DIR):
        print(f'No snapshot directory found: {SNAPSHOT_DIR}')
        sys.exit(1)
    # Find all subdirectories with timestamp names
    subdirs = [d for d in glob.glob(os.path.join(SNAPSHOT_DIR, '*')) if os.path.isdir(d)]
    if not subdirs:
        print('No snapshots found!')
        sys.exit(1)
    # Sort by timestamp descending
    latest_dir = sorted(subdirs, reverse=True)[0]
    sql_files = glob.glob(os.path.join(latest_dir, '*.sql'))
    if not sql_files:
        print(f'No SQL backup found in {latest_dir}')
        sys.exit(1)
    return sql_files[0]


def restore_snapshot(backup_file):
    print(f'Restoring integration DB from {backup_file}...')
    # Drop and recreate database
    drop_cmd = [
        'dropdb',
        '-h', DB_HOST,
        '-p', DB_PORT,
        '-U', DB_USER,
        DB_NAME
    ]
    create_cmd = [
        'createdb',
        '-h', DB_HOST,
        '-p', DB_PORT,
        '-U', DB_USER,
        DB_NAME
    ]
    restore_cmd = [
        'pg_restore',
        '-h', DB_HOST,
        '-p', DB_PORT,
        '-U', DB_USER,
        '-d', DB_NAME,
        '-v',
        backup_file
    ]
    env = os.environ.copy()
    env['PGPASSWORD'] = os.environ.get('PGPASSWORD', '')
    try:
        subprocess.run(drop_cmd, check=False, env=env)  # Ignore error if DB doesn't exist
        subprocess.run(create_cmd, check=True, env=env)
        subprocess.run(restore_cmd, check=True, env=env)
        print('Restore complete!')
    except subprocess.CalledProcessError as e:
        print(f'Restore failed: {e}')
        sys.exit(1)

def run_pytest():
    print('Running integration tests...')
    cmd = ['pytest', 'intg_tests/integration', '--maxfail=5', '--disable-warnings', '-v']
    env = os.environ.copy()
    env['PYTHONPATH'] = 'src'
    result = subprocess.run(cmd, env=env)
    sys.exit(result.returncode)


def main():
    backup_file = find_latest_snapshot()
    restore_snapshot(backup_file)
    print('Reloading integration test fixtures...')
    subprocess.run([
        'python3', '-m', 'intg_tests.db.test_db_manager', '--action', 'load_fixtures'
    ], check=True, env=dict(os.environ, PYTHONPATH='src'))
    run_pytest()

if __name__ == '__main__':
    main()
