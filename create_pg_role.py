import psycopg2
import os

# Usage: Set PG_SUPER_URL to a superuser connection string, or edit below
PG_SUPER_URL = os.getenv('PG_SUPER_URL', 'postgresql://jianjunchen@localhost:5432/postgres')
NEW_ROLE = os.getenv('PG_NEW_ROLE', 'postgres')
NEW_PASSWORD = os.getenv('PG_NEW_PASSWORD', 'changeme')
SUPERUSER = bool(os.getenv('PG_NEW_SUPERUSER', '1') == '1')

CREATE_ROLE_SQL = f"""
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '{NEW_ROLE}') THEN
        CREATE ROLE {NEW_ROLE} WITH LOGIN PASSWORD '{NEW_PASSWORD}'{' SUPERUSER' if SUPERUSER else ''};
    END IF;
END
$$;
"""

if __name__ == '__main__':
    conn = psycopg2.connect(PG_SUPER_URL)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(CREATE_ROLE_SQL)
    cur.close()
    conn.close()
    print(f"Role '{NEW_ROLE}' created (or already exists). Superuser: {SUPERUSER}")
