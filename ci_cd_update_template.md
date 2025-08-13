# CI/CD Workflow Update for PostgreSQL Test Database

## Instructions for Updating `.github/workflows/ci-cd.yaml`

To ensure your CI/CD pipeline uses the correct PostgreSQL test user credentials, you'll need to update your workflow file. Here's what you should add or modify:

### 1. Add Environment Variables for PostgreSQL

Look for the section where you set up environment variables for your test jobs and add these variables:

```yaml
env:
  POSTGRES_HOST: localhost
  POSTGRES_PORT: 5432
  POSTGRES_USER: test_user
  POSTGRES_PASSWORD: test_password
  POSTGRES_POOL_MIN: 1
  POSTGRES_POOL_MAX: 10
  POSTGRES_CMD_TIMEOUT: 60
```

### 2. Update PostgreSQL Service Configuration

If your workflow uses a PostgreSQL service container, update it to include the test user credentials:

```yaml
services:
  postgres:
    image: postgres:latest
    env:
      POSTGRES_USER: test_user
      POSTGRES_PASSWORD: test_password
      POSTGRES_DB: postgres
    ports:
      - 5432:5432
    options: >-
      --health-cmd pg_isready
      --health-interval 10s
      --health-timeout 5s
      --health-retries 5
```

### 3. Add Setup Step for PostgreSQL Test User

Add a step to ensure the test user has the necessary permissions:

```yaml
- name: Set up PostgreSQL test user
  run: |
    PGPASSWORD=test_password psql -h localhost -U test_user -d postgres -c "SELECT 'Connection test successful';"
```

### 4. Update Test Run Commands

If you have specific test run commands, make sure they use the environment variables:

```yaml
- name: Run tests
  run: |
    uv run pytest -xvs tests/
  env:
    POSTGRES_USER: test_user
    POSTGRES_PASSWORD: test_password
    POSTGRES_HOST: localhost
    POSTGRES_PORT: 5432
```

## Example Complete Job

Here's an example of how a complete job might look:

```yaml
test:
  runs-on: ubuntu-latest
  
  services:
    postgres:
      image: postgres:latest
      env:
        POSTGRES_USER: test_user
        POSTGRES_PASSWORD: test_password
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
    
    - name: Verify PostgreSQL connection
      run: |
        PGPASSWORD=test_password psql -h localhost -U test_user -d postgres -c "SELECT 'Connection test successful';"
    
    - name: Run tests
      run: |
        uv run pytest -xvs tests/
```

Make sure to adapt these examples to match your existing CI/CD workflow structure.
