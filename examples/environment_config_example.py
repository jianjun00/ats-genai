#!/usr/bin/env python3
"""
Environment Configuration Usage Examples
Demonstrates how to use the environment-specific gin configuration system
"""

import os
import sys
import logging
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from config.environment_config import Environment, load_gin_config, get_current_env, get_env_info
from config.validation import validate_current_config, validate_environment

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def example_automatic_environment_detection():
    """
    Example: Automatic environment detection and configuration loading
    """
    print("=" * 60)
    print("EXAMPLE 1: Automatic Environment Detection")
    print("=" * 60)

    # The system will automatically detect the environment based on:
    # 1. ATS_ENVIRONMENT environment variable
    # 2. Database connection settings (DB_HOST, DB_PORT)
    # 3. Container hostname
    # 4. Testing framework indicators

    # Load configuration (auto-detects environment)
    detected_env = load_gin_config()
    print(f"✅ Detected and loaded environment: {detected_env.value}")

    # Get current environment info
    current_env = get_current_env()
    print(f"📋 Current environment: {current_env.value}")

    # Get detailed environment information
    env_info = get_env_info()
    print("📊 Environment Details:")
    for key, value in env_info.items():
        print(f"   {key}: {value}")

def example_explicit_environment_loading():
    """
    Example: Explicitly loading specific environment configurations
    """
    print("\n" + "=" * 60)
    print("EXAMPLE 2: Explicit Environment Loading")
    print("=" * 60)

    environments_to_test = [Environment.DEVELOPMENT, Environment.INTEGRATION, Environment.PRODUCTION]

    for env in environments_to_test:
        print(f"\n🔧 Loading {env.value} environment...")

        # Load specific environment configuration
        loaded_env = load_gin_config(env, force_reload=True)
        print(f"✅ Successfully loaded {loaded_env.value} configuration")

        # Get environment-specific details
        env_info = get_env_info()
        config_file = env_info.get('config_file', 'Unknown')
        print(f"📄 Configuration file: {config_file}")

def example_configuration_with_environment_variables():
    """
    Example: How environment variables affect configuration loading
    """
    print("\n" + "=" * 60)
    print("EXAMPLE 3: Environment Variables Impact")
    print("=" * 60)

    # Show current environment variables that affect detection
    relevant_vars = ['ATS_ENVIRONMENT', 'DB_HOST', 'DB_PORT', 'HOSTNAME', 'PYTEST_CURRENT_TEST']

    print("🔍 Environment Variables (Detection Indicators):")
    for var in relevant_vars:
        value = os.getenv(var)
        status = "✅ SET" if value else "❌ NOT SET"
        print(f"   {var}: {value} ({status})")

    # Demonstrate environment variable override
    print("\n🧪 Testing environment variable override...")

    # Set explicit environment
    original_env = os.getenv('ATS_ENVIRONMENT')
    os.environ['ATS_ENVIRONMENT'] = 'dev'

    detected_env = load_gin_config(force_reload=True)
    print(f"✅ With ATS_ENVIRONMENT=dev, detected: {detected_env.value}")
    os.environ['ATS_ENVIRONMENT'] = 'intg'

    detected_env = load_gin_config(force_reload=True)
    print(f"✅ With ATS_ENVIRONMENT=intg, detected: {detected_env.value}")
    if original_env:
        os.environ['ATS_ENVIRONMENT'] = original_env
    else:
        os.environ.pop('ATS_ENVIRONMENT', None)

def example_configuration_validation():
    """
    Example: Validating configuration setup
    """
    print("\n" + "=" * 60)
    print("EXAMPLE 4: Configuration Validation")
    print("=" * 60)

    # Validate current configuration
    print("🔍 Validating current configuration...")
    result = validate_current_config()

    if result.is_valid:
        print("✅ Configuration validation PASSED")
    else:
        print("❌ Configuration validation FAILED")
        print("Errors:")
        for error in result.errors:
            print(f"   - {error}")

    if result.warnings:
        print("⚠️  Warnings:")
        for warning in result.warnings:
            print(f"   - {warning}")

    # Show validation summary
    if 'summary' in result.config_details:
        summary = result.config_details['summary']
        valid_envs = summary['valid_environments']
        total_envs = summary['total_environments']
        success_rate = summary['validation_success_rate']
        print(f"📊 Environment Validation Summary: {valid_envs}/{total_envs} environments valid ({success_rate:.1%})")

def example_environment_specific_behavior():
    """
    Example: How different environments change application behavior
    """
    print("\n" + "=" * 60)
    print("EXAMPLE 5: Environment-Specific Behavior")
    print("=" * 60)

    environments = [Environment.DEVELOPMENT, Environment.INTEGRATION, Environment.PRODUCTION]

    for env in environments:
        print(f"\n🎯 {env.value.upper()} Environment Configuration:")

        # Load environment
        load_gin_config(env, force_reload=True)

        # Get environment info
        env_info = get_env_info()

        # Show key differences
        print(f"   📄 Config file: {Path(env_info.get('config_file', '')).name}")
        print(f"   🔧 Environment: {env_info['current_environment']}")

        # Note: In a real application, you would demonstrate actual configuration differences
        # by importing and using gin-configured classes. For this example, we show the concept.

        behavior_notes = {
            'dev': [
                "• Smaller batch sizes for faster iteration",
                "• Longer timeouts for debugging",
                "• Limited symbol universe for testing",
                "• Debug-level logging",
                "• Local database connections"
            ],
            'intg': [
                "• Moderate batch sizes for comprehensive testing",
                "• Production-like timeouts",
                "• Broader symbol coverage for testing",
                "• Info-level logging",
                "• Integration database connections"
            ],
            'prod': [
                "• Large batch sizes for throughput",
                "• Strict timeouts for performance",
                "• Full market symbol universe",
                "• Warning-level logging only",
                "• Production database cluster"
            ]
        }

        notes = behavior_notes.get(env.value, ["• Environment-specific configuration loaded"])
        for note in notes:
            print(f"   {note}")

def example_gin_parameter_access():
    """
    Example: Accessing gin-configured parameters in application code
    """
    print("\n" + "=" * 60)
    print("EXAMPLE 6: Accessing Gin-Configured Parameters")
    print("=" * 60)

    print("💡 How to use gin-configured parameters in your code:")
    print()

    # Show code example
    code_example = '''
# In your application code:

import gin
from dataclasses import dataclass

@gin.configurable
@dataclass
class DatabaseConfig:
    host: str = 'localhost'
    port: int = 5432
    user: str = 'postgres'
    password: str = 'password'
    database: str = 'dev_db'

@gin.configurable
@dataclass
class APIConfig:
    default_timeout: int = 30
    batch_size: int = 100
    max_retries: int = 3

# Usage in your application:
def create_database_connection():
    # Gin will inject the environment-specific values
    db_config = DatabaseConfig()

    print(f"Connecting to {db_config.host}:{db_config.port}")
    print(f"Database: {db_config.database}")
    # ... create actual connection

def configure_api_client():
    api_config = APIConfig()

    print(f"API timeout: {api_config.default_timeout}s")
    print(f"Batch size: {api_config.batch_size}")
    # ... configure API client
'''

    print(code_example)

    print("🔄 The gin configuration system will automatically:")
    print("   • Load the appropriate environment configuration file")
    print("   • Override default values with environment-specific values")
    print("   • Provide consistent parameter access across all modules")

def run_all_examples():
    """
    Run all configuration examples
    """
    print("🚀 ATS Platform Environment Configuration Examples")
    print("🏗️  Demonstrating environment-specific gin configuration system")

    example_automatic_environment_detection()
    example_explicit_environment_loading()
    example_configuration_with_environment_variables()
    example_configuration_validation()
    example_environment_specific_behavior()
    example_gin_parameter_access()

    print("\n" + "=" * 60)
    print("✅ ALL EXAMPLES COMPLETED SUCCESSFULLY")
    print("=" * 60)
    print("📚 Next Steps:")
    print("   1. Set ATS_ENVIRONMENT variable for your deployment")
    print("   2. Customize environment-specific configuration files")
    print("   3. Use @gin.configurable decorator in your classes")
    print("   4. Load configuration at application startup")
    print("   5. Validate configuration in CI/CD pipeline")

if __name__ == "__main__":
    run_all_examples()