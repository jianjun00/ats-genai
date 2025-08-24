#!/usr/bin/env python3

"""Environment configuration"""
import os

class Environment:
    def __init__(self):
        self.db_host = os.getenv('DB_HOST', 'postgres-simple')
        self.db_port = os.getenv('DB_PORT', '5432')
        self.db_user = os.getenv('DB_USER', 'postgres')
        self.db_password = os.getenv('DB_PASSWORD', 'dev_password')
        self.db_name = os.getenv('DB_NAME', 'dev_db')

    def get_database_url(self):
        return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"


if __name__ == "__main__":
    env = Environment()
    print(f"Database URL: {env.get_database_url()}")
    print("✅ Environment configuration loaded successfully")