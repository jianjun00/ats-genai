#!/usr/bin/env python3
import argparse
import os
import sys
from datetime import datetime
import subprocess

# Map environment to database name (customize as needed)
DB_NAMES = {
    'test': 'test_trading_db',
    'intg': 'intg_trading_db',
    'prod': 'prod_trading_db',
}

# Optionally, map environment to user/host/port if needed
DB_USER = os.environ.get('PGUSER', 'postgres')
DB_HOST = os.environ.get('PGHOST', 'localhost')
DB_PORT = os.environ.get('PGPORT', '5432')


def main():
    parser = argparse.ArgumentParser(description='Backup database by environment.')
    parser.add_argument('--env', required=True, choices=DB_NAMES.keys(), help='Environment to backup (test/intg/prod)')
    args = parser.parse_args()

    db_name = DB_NAMES[args.env]
    now = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_dir = os.path.join(os.path.dirname(__file__), '..', 'db', 'snapshot', args.env, now)
    os.makedirs(backup_dir, exist_ok=True)
    backup_file = os.path.join(backup_dir, f'{db_name}.sql')

    print(f'Backing up database {db_name} to {backup_file}...')

    cmd = [
        'pg_dump',
        '-h', DB_HOST,
        '-p', DB_PORT,
        '-U', DB_USER,
        '-F', 'c',  # Custom format
        '-b',       # Include blobs
        '-v',       # Verbose
        '-f', backup_file,
        db_name
    ]

    env = os.environ.copy()
    env['PGPASSWORD'] = os.environ.get('PGPASSWORD', '')
    try:
        subprocess.run(cmd, check=True, env=env)
        print('Backup completed successfully!')
    except subprocess.CalledProcessError as e:
        print(f'Backup failed: {e}')
        sys.exit(1)

if __name__ == '__main__':
    main()
