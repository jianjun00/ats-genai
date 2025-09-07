#!/usr/bin/env python3
"""
Test WSL System Monitoring and Slack Alerts

This script helps test the monitoring system by:
1. Testing Slack webhook connectivity
2. Sending sample alerts with different severity levels
3. Simulating system stress conditions for testing
4. Validating configuration files
"""

import os
import sys
import json
import time
import argparse
from pathlib import Path

# Add the monitoring directory to the path
sys.path.insert(0, str(Path(__file__).parent))

try:
    from wsl_system_monitor import WSLSystemMonitor, SystemMetrics, SlackNotifier
except ImportError as e:
    print(f"❌ Failed to import monitoring modules: {e}")
    print("Make sure you're running this from the correct directory")
    sys.exit(1)

def test_slack_connectivity(webhook_url: str) -> bool:
    """Test basic Slack webhook connectivity."""
    print("🔗 Testing Slack webhook connectivity...")

    try:
        notifier = SlackNotifier(webhook_url)

        # Create dummy metrics for testing
        from datetime import datetime
        import socket

        test_metrics = SystemMetrics(
            timestamp=datetime.now(),
            hostname=socket.gethostname(),
            cpu_percent=25.0,
            cpu_count=8,
            load_avg=[1.2, 1.1, 1.0],
            memory_total=16_000_000_000,
            memory_available=8_000_000_000,
            memory_percent=50.0,
            memory_used=8_000_000_000,
            swap_total=4_000_000_000,
            swap_used=0,
            swap_percent=0.0,
            disk_total=500_000_000_000,
            disk_used=250_000_000_000,
            disk_free=250_000_000_000,
            disk_percent=50.0,
            network_sent=1_000_000,
            network_recv=2_000_000,
            process_count=150,
            docker_containers=5,
            docker_running=3,
            postgres_connections=15,
            postgres_status="connected",
            ats_backfill_active=True,
            ats_data_size_gb=45.2
        )

        success = notifier.send_alert(
            alert_type="connectivity_test",
            title="WSL Monitor Connectivity Test",
            message="This is a connectivity test from the WSL System Monitor. If you see this message, the Slack integration is working correctly! 🎉",
            metrics=test_metrics,
            severity="info"
        )

        if success:
            print("✅ Slack webhook connectivity test PASSED")
            return True
        else:
            print("❌ Slack webhook connectivity test FAILED")
            return False

    except Exception as e:
        print(f"❌ Slack connectivity test error: {e}")
        return False

def test_alert_severities(webhook_url: str, config_file: str = None) -> bool:
    """Test different alert severity levels."""
    print("🚨 Testing different alert severity levels...")

    try:
        monitor = WSLSystemMonitor(webhook_url, config_file)

        # Get current system metrics
        metrics = monitor.get_system_metrics()

        # Test different severity levels
        test_alerts = [
            {
                'type': 'test_info',
                'severity': 'info',
                'title': 'Info Level Test',
                'message': 'This is an informational alert test. System is operating normally.'
            },
            {
                'type': 'test_warning',
                'severity': 'warning',
                'title': 'Warning Level Test',
                'message': 'This is a warning level alert test. Some metric is approaching threshold.'
            },
            {
                'type': 'test_critical',
                'severity': 'critical',
                'title': 'Critical Level Test',
                'message': '🔥 This is a critical level alert test. Immediate attention required!'
            },
            {
                'type': 'test_recovery',
                'severity': 'recovery',
                'title': 'Recovery Test',
                'message': '✅ This is a recovery alert test. System has returned to normal.'
            }
        ]

        success_count = 0
        for alert in test_alerts:
            print(f"  📤 Sending {alert['severity']} alert...")
            success = monitor.slack_notifier.send_alert(
                alert_type=alert['type'],
                title=alert['title'],
                message=alert['message'],
                metrics=metrics,
                severity=alert['severity']
            )

            if success:
                success_count += 1
                print(f"    ✅ {alert['severity']} alert sent")
            else:
                print(f"    ❌ {alert['severity']} alert failed")

            # Wait between alerts to avoid rate limiting
            time.sleep(2)

        print(f"📊 Alert test results: {success_count}/{len(test_alerts)} alerts sent successfully")
        return success_count == len(test_alerts)

    except Exception as e:
        print(f"❌ Alert severity test error: {e}")
        return False

def simulate_stress_conditions(webhook_url: str, config_file: str = None) -> bool:
    """Simulate stress conditions to test alert thresholds."""
    print("⚡ Testing stress condition detection...")

    try:
        monitor = WSLSystemMonitor(webhook_url, config_file)

        # Get current metrics as baseline
        real_metrics = monitor.get_system_metrics()

        # Create simulated stress conditions
        stress_scenarios = [
            {
                'name': 'High CPU Usage',
                'metrics_override': {'cpu_percent': 95.0},
                'expected_alert': 'cpu_critical'
            },
            {
                'name': 'High Memory Usage',
                'metrics_override': {'memory_percent': 96.0, 'memory_available': 100_000_000},
                'expected_alert': 'memory_critical'
            },
            {
                'name': 'High Disk Usage',
                'metrics_override': {'disk_percent': 92.0, 'disk_free': 1_000_000_000},
                'expected_alert': 'disk_critical'
            }
        ]

        for scenario in stress_scenarios:
            print(f"  🧪 Testing scenario: {scenario['name']}")

            # Create modified metrics
            test_metrics = real_metrics
            for key, value in scenario['metrics_override'].items():
                setattr(test_metrics, key, value)

            # Analyze stress conditions
            stress_alerts = monitor.analyze_stress_conditions(test_metrics)

            # Check if expected alert was generated
            expected_found = any(alert['type'].startswith(scenario['expected_alert'])
                               for alert in stress_alerts)

            if expected_found:
                print(f"    ✅ Stress condition detected correctly")

                # Send the actual alert
                for alert in stress_alerts:
                    if alert['type'].startswith(scenario['expected_alert']):
                        monitor.slack_notifier.send_alert(
                            alert_type=f"test_{alert['type']}",
                            title=f"TEST: {alert['title']}",
                            message=f"SIMULATED: {alert['message']}",
                            metrics=test_metrics,
                            severity=alert['severity']
                        )
                        break
            else:
                print(f"    ❌ Expected stress condition not detected")

            time.sleep(3)

        print("✅ Stress condition testing completed")
        return True

    except Exception as e:
        print(f"❌ Stress condition test error: {e}")
        return False

def validate_configuration(config_file: str) -> bool:
    """Validate the monitoring configuration file."""
    print(f"⚙️  Validating configuration file: {config_file}")

    try:
        if not Path(config_file).exists():
            print(f"❌ Configuration file not found: {config_file}")
            return False

        with open(config_file, 'r') as f:
            config = json.load(f)

        # Check required sections
        required_sections = ['thresholds', 'alert_settings', 'monitoring_targets']
        missing_sections = [section for section in required_sections
                          if section not in config]

        if missing_sections:
            print(f"❌ Missing configuration sections: {missing_sections}")
            return False

        # Check threshold values are reasonable
        thresholds = config['thresholds']
        threshold_checks = [
            ('cpu_warning', 0, 100),
            ('cpu_critical', 0, 100),
            ('memory_warning', 0, 100),
            ('memory_critical', 0, 100),
            ('disk_warning', 0, 100),
            ('disk_critical', 0, 100)
        ]

        for threshold_name, min_val, max_val in threshold_checks:
            if threshold_name in thresholds:
                value = thresholds[threshold_name]
                if not (min_val <= value <= max_val):
                    print(f"❌ Invalid threshold value: {threshold_name} = {value} (should be {min_val}-{max_val})")
                    return False

        # Check that critical thresholds are higher than warning thresholds
        threshold_pairs = [
            ('cpu_warning', 'cpu_critical'),
            ('memory_warning', 'memory_critical'),
            ('disk_warning', 'disk_critical')
        ]

        for warning_key, critical_key in threshold_pairs:
            if (warning_key in thresholds and critical_key in thresholds and
                thresholds[warning_key] >= thresholds[critical_key]):
                print(f"❌ Critical threshold should be higher than warning: "
                      f"{warning_key}({thresholds[warning_key]}) >= {critical_key}({thresholds[critical_key]})")
                return False

        print("✅ Configuration validation passed")
        return True

    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON in configuration file: {e}")
        return False
    except Exception as e:
        print(f"❌ Configuration validation error: {e}")
        return False

def test_system_metrics_collection() -> bool:
    """Test system metrics collection functionality."""
    print("📊 Testing system metrics collection...")

    try:
        # Create temporary monitor instance without Slack
        monitor = WSLSystemMonitor("dummy_webhook")

        # Collect metrics
        metrics = monitor.get_system_metrics()

        # Validate metrics
        checks = [
            ('hostname', str, lambda x: len(x) > 0),
            ('cpu_percent', (int, float), lambda x: 0 <= x <= 100),
            ('memory_percent', (int, float), lambda x: 0 <= x <= 100),
            ('disk_percent', (int, float), lambda x: 0 <= x <= 100),
            ('process_count', int, lambda x: x > 0),
        ]

        for field_name, expected_type, validation_func in checks:
            if hasattr(metrics, field_name):
                value = getattr(metrics, field_name)

                if not isinstance(value, expected_type):
                    print(f"❌ {field_name} has wrong type: {type(value)} (expected {expected_type})")
                    return False

                if not validation_func(value):
                    print(f"❌ {field_name} failed validation: {value}")
                    return False
            else:
                print(f"❌ Missing metric field: {field_name}")
                return False

        print("✅ System metrics collection test passed")
        print(f"    System: {metrics.hostname}")
        print(f"    CPU: {metrics.cpu_percent:.1f}% ({metrics.cpu_count} cores)")
        print(f"    Memory: {metrics.memory_percent:.1f}% ({metrics.memory_available//1024//1024:,}MB available)")
        print(f"    Disk: {metrics.disk_percent:.1f}% ({metrics.disk_free//1024//1024//1024:.1f}GB free)")
        print(f"    Processes: {metrics.process_count}")
        print(f"    Docker: {metrics.docker_running}/{metrics.docker_containers} containers running")
        print(f"    Database: {metrics.postgres_status} ({metrics.postgres_connections} connections)")
        print(f"    ATS Backfill: {'Active' if metrics.ats_backfill_active else 'Inactive'}")

        return True

    except Exception as e:
        print(f"❌ System metrics collection error: {e}")
        return False

def main():
    """Main test runner."""
    parser = argparse.ArgumentParser(description="Test WSL System Monitor")

    parser.add_argument(
        "--slack-webhook",
        required=True,
        help="Slack webhook URL for testing"
    )

    parser.add_argument(
        "--config-file",
        default="monitor_config.json",
        help="Configuration file path"
    )

    parser.add_argument(
        "--test",
        choices=['all', 'connectivity', 'alerts', 'stress', 'config', 'metrics'],
        default='all',
        help="Which tests to run"
    )

    args = parser.parse_args()

    print("🧪 WSL System Monitor Test Suite")
    print("=" * 50)

    # Test results tracking
    results = {}

    # Run selected tests
    if args.test in ['all', 'config']:
        results['config'] = validate_configuration(args.config_file)

    if args.test in ['all', 'metrics']:
        results['metrics'] = test_system_metrics_collection()

    if args.test in ['all', 'connectivity']:
        results['connectivity'] = test_slack_connectivity(args.slack_webhook)

    if args.test in ['all', 'alerts']:
        results['alerts'] = test_alert_severities(args.slack_webhook, args.config_file)

    if args.test in ['all', 'stress']:
        results['stress'] = simulate_stress_conditions(args.slack_webhook, args.config_file)

    # Summary
    print("\n" + "=" * 50)
    print("📋 Test Results Summary")
    print("=" * 50)

    passed = 0
    total = len(results)

    for test_name, result in results.items():
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"  {test_name.capitalize():15} {status}")
        if result:
            passed += 1

    print(f"\nOverall: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All tests passed! Your monitoring system is ready!")
        return 0
    else:
        print("⚠️  Some tests failed. Please check the configuration and try again.")
        return 1

if __name__ == "__main__":
    exit(main())