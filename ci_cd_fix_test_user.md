# Fix for CI/CD PostgreSQL Test User Authentication

## Problem
The CI/CD workflow is failing with the error:
```
Connection matched pg_hba.conf line 100: "host all all all scram-sha-256"
FATAL: password authentication failed for user "test_user"
DETAIL: Role "test_user" does not exist.
```

## Solution
The PostgreSQL service in your CI/CD environment doesn't automatically create the `test_user` role. You need to add a step to create this user before running your tests.

### Add a Step to Create the Test User

Add this step after your PostgreSQL service is started but before running tests:

```yaml
- name: Create PostgreSQL test user
  run: |
    # Wait for PostgreSQL to be ready
    sleep 5
    
    # Create test_user with password and grant necessary permissions
    PGPASSWORD=${{ env.POSTGRES_PASSWORD }} psql -h localhost -U postgres -d postgres -c "
      CREATE USER test_user WITH PASSWORD 'test_password';
      ALTER USER test_user WITH SUPERUSER;
    "
    
    # Verify the user was created successfully
    PGPASSWORD=test_password psql -h localhost -U test_user -d postgres -c "SELECT 'Test user connection successful';"
```

### Update PostgreSQL Service Configuration

Make sure your PostgreSQL service is configured with the default `postgres` user:

```yaml
services:
  postgres:
    image: postgres:latest
    env:
      POSTGRES_PASSWORD: password
      POSTGRES_DB: postgres
    ports:
      - 5432:5432
    options: >-
      --health-cmd pg_isready
      --health-interval 10s
      --health-timeout 5s
      --health-retries 5
```

### Complete Example

Here's how your workflow might look:

```yaml
name: CI/CD

on:
  push:
    branches: [ main ]
  pull_request:
    branches: [ main ]

jobs:
  test:
    runs-on: ubuntu-latest
    
    services:
      postgres:
        image: postgres:latest
        env:
          POSTGRES_PASSWORD: password
          POSTGRES_DB: postgres
        ports:
          - 5432:5432
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
    
    env:
      POSTGRES_HOST: localhost
      POSTGRES_PORT: 5432
      POSTGRES_USER: test_user
      POSTGRES_PASSWORD: test_password
      POSTGRES_POOL_MIN: 1
      POSTGRES_POOL_MAX: 10
      POSTGRES_CMD_TIMEOUT: 60
    
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.12'
      
      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install uv
          uv pip install -r requirements.txt
          uv pip install pytest
      
      - name: Create PostgreSQL test user
        run: |
          # Wait for PostgreSQL to be ready
          sleep 5
          
          # Create test_user with password and grant necessary permissions
          PGPASSWORD=password psql -h localhost -U postgres -d postgres -c "
            CREATE USER test_user WITH PASSWORD 'test_password';
            ALTER USER test_user WITH SUPERUSER;
          "
          
          # Verify the user was created successfully
          PGPASSWORD=test_password psql -h localhost -U test_user -d postgres -c "SELECT 'Test user connection successful';"
      
      - name: Run tests
        run: |
          uv run pytest -xvs tests/
```

## Important Notes

1. The PostgreSQL service in GitHub Actions starts with a default `postgres` user, not `test_user`.
2. You need to explicitly create the `test_user` role in a step before running your tests.
3. Make sure to wait for PostgreSQL to be fully ready before attempting to create the user.
4. The `PGPASSWORD` environment variable is used to pass the password to the `psql` command.

Apply these changes to your `.github/workflows/ci-cd.yaml` file to fix the authentication issue.
