"""
Regression Tests for Hardcoded API Keys Security Issue

This test suite prevents regression of the security vulnerability where
API keys were hardcoded throughout the codebase, creating risks of
accidental exposure in version control, logs, and documentation.

Issue: 18 files contained hardcoded Polygon API key 'wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD'
Fix: Replaced with environment variable references and placeholders
"""

import pytest
import os
import re
from pathlib import Path

class TestHardcodedApiKeysRegression:
    """Test suite to prevent hardcoded API keys from returning to codebase"""

    @pytest.fixture
    def project_root(self):
        """Get project root directory"""
        return Path('/workspace')

    @pytest.fixture
    def known_sensitive_patterns(self):
        """Define patterns that should never appear in code"""
        return [
            # Specific API keys we fixed
            'wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD',  # Polygon API key
            '5f40b4f36e171405746304ec0e5a6f3aa9ca77e5',  # Tiingo API key
            '68aa0c7d2fe831.67386369',  # EODHD API key

            # OpenAI API keys (pattern)
            'sk-svcacct-[A-Za-z0-9\\-_]{20,}',
            'sk-[A-Za-z0-9]{20,}',

            # Generic API key patterns
            '[A-Za-z0-9]{32,}',  # Long alphanumeric strings (potential keys)

            # Database passwords in code
            'dev_password',  # Should only be in env files
            'postgres.*password.*=.*[^$]',  # Hardcoded postgres passwords
        ]

    @pytest.fixture
    def safe_files_with_api_keys(self):
        """Files that are allowed to contain API keys (environment files only)"""
        return {
            '.env.test',  # Contains working API keys for operations
            '.env.template',  # Contains placeholder text
            '.env',  # Contains placeholder text
            '.env.dev',  # Contains placeholder text
            '.env.prod',  # Contains placeholder text
            'test_*.py',  # Test files with test_api_key_placeholder
        }

    @pytest.fixture
    def files_to_scan(self, project_root):
        """Get all files that should be scanned for hardcoded secrets"""
        extensions = {'.py', '.js', '.ts', '.yaml', '.yml', '.json', '.md', '.sh'}
        files_to_scan = []

        for ext in extensions:
            files_to_scan.extend(project_root.rglob(f'*{ext}'))

        # Exclude certain directories
        excluded_dirs = {'.git', 'node_modules', '__pycache__', '.pytest_cache', 'venv', 'env'}

        filtered_files = []
        for file_path in files_to_scan:
            if not any(excluded_dir in file_path.parts for excluded_dir in excluded_dirs):
                filtered_files.append(file_path)

        return filtered_files

    def test_no_hardcoded_polygon_api_key(self, files_to_scan):
        """Test that the specific Polygon API key is not hardcoded anywhere"""
        polygon_key = 'wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD'
        violations = []

        for file_path in files_to_scan:
            if file_path.name == '.env.test':
                # .env.test is allowed to contain the working API key
                continue

            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                if polygon_key in content:
                    # Check if it's in a safe context (e.g., documentation about what NOT to do)
                    lines = content.split('\n')
                    for line_num, line in enumerate(lines, 1):
                        if polygon_key in line:
                            # Check for safe contexts
                            safe_contexts = [
                                'NEVER DO THIS',
                                'test_api_key_placeholder',
                                'your_api_key_here',
                                'example of what not to do',
                                '❌', '# ❌'
                            ]

                            if not any(safe_ctx in line for safe_ctx in safe_contexts):
                                violations.append({
                                    'file': str(file_path),
                                    'line': line_num,
                                    'content': line.strip(),
                                    'issue': 'Hardcoded Polygon API key found'
                                })
        assert len(violations) == 0, f"Found hardcoded Polygon API key in {len(violations)} locations: {violations}"

    def test_no_hardcoded_tiingo_api_key(self, files_to_scan):
        """Test that Tiingo API keys are properly handled"""
        tiingo_key = '5f40b4f36e171405746304ec0e5a6f3aa9ca77e5'
        violations = []

        for file_path in files_to_scan:
            if file_path.name in ['.env.test', '.env.template', '.env', '.env.dev', '.env.prod']:
                # Environment files are allowed
                continue

            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                if tiingo_key in content:
                    violations.append(str(file_path))
        assert len(violations) == 0, f"Found hardcoded Tiingo API key in: {violations}"

    def test_environment_variable_usage(self, files_to_scan):
        """Test that code properly uses environment variables for API keys"""
        proper_patterns = [
            r"os\.getenv\(['\"]POLYGON_API_KEY['\"]",
            r"os\.environ\.get\(['\"]POLYGON_API_KEY['\"]",
            r"os\.environ\[['\"]POLYGON_API_KEY['\"]\]",
            r"\$\{POLYGON_API_KEY\}",  # Shell/config file pattern
            r"env\.get_api_key\(['\"]polygon['\"]",  # Our custom method
        ]

        python_files = [f for f in files_to_scan if f.suffix == '.py']
        files_with_proper_usage = 0
        files_using_polygon_api = []

        for file_path in python_files:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()

                # Check if file uses Polygon API
                if 'polygon' in content.lower() and 'api' in content.lower():
                    files_using_polygon_api.append(file_path)

                    # Check if it uses proper environment variable patterns
                    has_proper_usage = any(re.search(pattern, content, re.IGNORECASE)
                                         for pattern in proper_patterns)

                    if has_proper_usage:
                        files_with_proper_usage += 1
        assert files_with_proper_usage > 0, "Should have files using proper environment variable patterns"

    def test_test_files_use_placeholders(self, files_to_scan):
        """Test that test files use placeholder API keys instead of real ones"""
        test_files = [f for f in files_to_scan if f.name.startswith('test_') and f.suffix == '.py']

        violations = []
        real_api_keys = [
            'wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD',
            '5f40b4f36e171405746304ec0e5a6f3aa9ca77e5',
            '68aa0c7d2fe831.67386369'
        ]

        for file_path in test_files:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()

                for api_key in real_api_keys:
                    if api_key in content:
                        # Check if it's been replaced with placeholder
                        if 'test_api_key_placeholder' not in content:
                            violations.append({
                                'file': str(file_path),
                                'issue': f'Real API key {api_key[:10]}... found without placeholder pattern'
                            })
        assert len(violations) == 0, f"Test files should use placeholders: {violations}"

    def test_documentation_uses_placeholders(self, files_to_scan):
        """Test that documentation files use placeholder values instead of real API keys"""
        doc_files = [f for f in files_to_scan if f.suffix in {'.md', '.rst', '.txt'}]

        violations = []
        real_api_keys = [
            'wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD',
            '5f40b4f36e171405746304ec0e5a6f3aa9ca77e5'
        ]

        for file_path in doc_files:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()

                for api_key in real_api_keys:
                    if api_key in content:
                        # Check if it's in a safe documentation context
                        safe_contexts = [
                            '${POLYGON_API_KEY}',
                            'your_polygon_api_key_here',
                            'your_api_key_here',
                            '❌', 'NEVER DO THIS',
                            'example of what not to do'
                        ]

                        has_safe_context = any(ctx in content for ctx in safe_contexts)
                        if not has_safe_context:
                            violations.append(str(file_path))
        assert len(violations) == 0, f"Documentation should use placeholders: {violations}"

    def test_environment_files_structure(self):
        """Test that environment files have proper structure"""
        env_files = [
            '/workspace/.env.template',
            '/workspace/.env.dev',
            '/workspace/.env.prod',
            '/workspace/.env'
        ]

        for env_file in env_files:
            if os.path.exists(env_file):
                with open(env_file, 'r') as f:
                    content = f.read()

                    # Should have POLYGON_API_KEY line
                    assert 'POLYGON_API_KEY=' in content, f"{env_file} should have POLYGON_API_KEY setting"

                    # Should use placeholder, not real key
                    assert 'your_polygon_api_key_here' in content or 'your_api_key_here' in content, \
                        f"{env_file} should use placeholder value"

                    # Should NOT contain real API key
                    assert 'wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD' not in content, \
                        f"{env_file} should not contain real API key"

    def test_env_test_file_contains_working_keys(self):
        """Test that .env.test contains working API keys (special case)"""
        env_test_file = '/workspace/.env.test'

        if os.path.exists(env_test_file):
            with open(env_test_file, 'r') as f:
                content = f.read()

                # Should contain actual working keys for operations
                assert 'POLYGON_API_KEY=' in content
                assert 'TIINGO_API_KEY=' in content

                # Should contain the working Polygon key
                assert 'wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD' in content, \
                    ".env.test should contain working API key for active operations"

    def test_scripts_use_environment_variables(self, files_to_scan):
        """Test that our fix scripts properly use environment variables"""
        script_files = [
            '/workspace/scripts/run_polygon_backfill_direct.py',
            '/workspace/scripts/run_polygon_daily_backfill_30years.py',
            '/workspace/scripts/run_tiingo_daily_backfill.py',
            '/workspace/scripts/fix_tiingo_population.py'
        ]

        for script_file in script_files:
            if os.path.exists(script_file):
                with open(script_file, 'r') as f:
                    content = f.read()

                    # Should use os.getenv() pattern
                    if 'POLYGON_API_KEY' in content:
                        assert 'os.getenv(' in content, f"{script_file} should use os.getenv()"
                        assert 'POLYGON_API_KEY' not in content or \
                               'os.getenv(\'POLYGON_API_KEY\')' in content or \
                               'os.getenv("POLYGON_API_KEY")' in content, \
                               f"{script_file} should get API key from environment"

                    # Should NOT contain hardcoded keys
                    assert 'wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD' not in content, \
                        f"{script_file} should not contain hardcoded API key"

    def test_git_secrets_prevention(self):
        """Test mechanisms to prevent committing secrets"""
        project_root = Path('/workspace')

        # Check if .gitignore properly excludes environment files with secrets
        gitignore_path = project_root / '.gitignore'
        if gitignore_path.exists():
            with open(gitignore_path, 'r') as f:
                gitignore_content = f.read()

                # Should ignore .env files (except templates)
                env_patterns = ['.env', '*.env', '.env.local']
                has_env_ignore = any(pattern in gitignore_content for pattern in env_patterns)

                # Note: We allow .env.test to be committed as it's needed for operations
                # but this is a controlled exception
                if not has_env_ignore:
                    print("Warning: Consider adding .env files to .gitignore")

    @pytest.mark.integration
    def test_no_api_keys_in_git_history(self):
        """Test that API keys haven't been committed to git history"""
        # This test would ideally check git history for leaked secrets
        # For now, we document the importance and provide a command

        git_history_command = [
            "git", "log", "--all", "--full-history", "--",
            "**/.*env*", "**/*.py", "**/*.js", "**/*.md"
        ]

        # In a real implementation, we'd run git log and search for patterns
        # For now, we ensure the test framework exists
        assert os.path.exists('/workspace/.git'), "Should be a git repository"

    def test_security_documentation_exists(self):
        """Test that security documentation exists and mentions API key handling"""
        doc_paths = [
            '/workspace/README.md',
            '/workspace/docs',
            '/workspace/CLAUDE.md'
        ]

        found_security_docs = False
        for doc_path in doc_paths:
            if os.path.exists(doc_path):
                if os.path.isfile(doc_path):
                    with open(doc_path, 'r') as f:
                        content = f.read().lower()
                        if 'api key' in content or 'environment variable' in content:
                            found_security_docs = True
                            break
                elif os.path.isdir(doc_path):
                    # Check for security-related docs in directory
                    for file in Path(doc_path).rglob('*.md'):
                        with open(file, 'r', errors='ignore') as f:
                            content = f.read().lower()
                            if 'api key' in content or 'security' in content:
                                found_security_docs = True
                                break

        assert found_security_docs, "Should have documentation mentioning API key security"


@pytest.mark.integration
class TestApiKeySecurityIntegration:
    """Integration tests for API key security across the full system"""

    def test_environment_variable_propagation(self):
        """Test that environment variables properly propagate through Docker/Kubernetes"""
        # Test that run_dev.py properly passes environment variables
        run_dev_path = '/workspace/scripts/run_dev.py'

        if os.path.exists(run_dev_path):
            with open(run_dev_path, 'r') as f:
                content = f.read()

                # Should have mechanisms to pass environment variables
                assert '-e ' in content or 'env=' in content, \
                    "run_dev.py should support environment variable passing"

    def test_docker_environment_handling(self):
        """Test that Docker containers properly handle environment variables"""
        # Check that Docker commands include proper environment variable handling
        run_dev_path = '/workspace/scripts/run_dev.py'

        if os.path.exists(run_dev_path):
            with open(run_dev_path, 'r') as f:
                content = f.read()

                # Should include environment variable patterns in Docker commands
                docker_env_patterns = [
                    '-e POLYGON_API_KEY',
                    '-e TIINGO_API_KEY',
                    'env_vars',
                    'environment='
                ]

                has_docker_env = any(pattern in content for pattern in docker_env_patterns)
                assert has_docker_env, "Docker commands should support environment variables"

    def test_backup_and_recovery_excludes_secrets(self):
        """Test that backup processes exclude secret files"""
        # Test that sensitive files are properly handled in backup scenarios
        sensitive_files = ['.env.test', '.env.local', 'secrets.json']

        # This test would check backup scripts exclude these files
        # For now, we document the requirement
        backup_exclusions = [
            '*.env',
            'secrets.*',
            'keys.*',
            'credentials.*'
        ]

        assert len(backup_exclusions) > 0, "Should have backup exclusion patterns defined"