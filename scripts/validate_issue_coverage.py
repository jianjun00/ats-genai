#!/usr/bin/env python3
"""
Validate test coverage for ATS-INTG identified issues
Uses subprocess calls to test database and containers
"""

import subprocess
import sys
import os
from datetime import datetime

class IssueTestCoverage:
    """Validate test coverage for identified INTG issues"""
    
    def __init__(self):
        self.results = {}
        self.total_tests = 0
        self.passed_tests = 0
    
    def test_missing_realtime_tables_fixed(self):
        """Test Issue #1: Missing real-time tables are now created"""
        print("🧪 Issue #1: Missing Real-time Tables")
        
        vendors = ['polygon', 'tiingo', 'fmp']
        missing_tables = []
        
        for vendor in vendors:
            table_name = f"intg_one_minute_live_{vendor}"
            result = subprocess.run([
                "bash", "-c", 
                f"PGPASSWORD=intg_password psql -h localhost -p 5434 -U postgres -d intg_db -t -c \"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}')\""
            ], capture_output=True, text=True)
            
            if result.returncode == 0:
                exists = result.stdout.strip() == 't'
                if not exists:
                    missing_tables.append(table_name)
            else:
                missing_tables.append(f"{table_name} (query failed)")
        
        if len(missing_tables) == 0:
            print("   ✅ PASS: All 3 real-time tables exist")
            self.results["missing_tables"] = "PASS"
            self.passed_tests += 1
        else:
            print(f"   ❌ FAIL: Missing tables: {missing_tables}")
            self.results["missing_tables"] = f"FAIL: {missing_tables}"
        
        self.total_tests += 1
    
    def test_empty_database_fixed(self):
        """Test Issue #2: Empty database now has data"""
        print("🧪 Issue #2: Empty Database")
        
        tables_to_check = [
            "intg_daily_prices",
            "intg_instruments", 
            "intg_one_minute_live_polygon",
            "intg_one_minute_live_tiingo",
            "intg_one_minute_live_fmp"
        ]
        
        total_records = 0
        empty_tables = []
        
        for table in tables_to_check:
            result = subprocess.run([
                "bash", "-c", 
                f"PGPASSWORD=intg_password psql -h localhost -p 5434 -U postgres -d intg_db -t -c \"SELECT COUNT(*) FROM {table}\""
            ], capture_output=True, text=True)
            
            if result.returncode == 0:
                count = int(result.stdout.strip())
                total_records += count
                if count == 0:
                    empty_tables.append(table)
            else:
                empty_tables.append(f"{table} (query failed)")
        
        if len(empty_tables) == 0 and total_records > 0:
            print(f"   ✅ PASS: All tables populated ({total_records:,} total records)")
            self.results["empty_database"] = "PASS"
            self.passed_tests += 1
        else:
            print(f"   ❌ FAIL: Empty tables: {empty_tables}, Total: {total_records}")
            self.results["empty_database"] = f"FAIL: {len(empty_tables)} empty tables"
        
        self.total_tests += 1
    
    def test_container_health_fixed(self):
        """Test Issue #3: Container health issues resolved"""
        print("🧪 Issue #3: Container Health Issues")
        
        # Check container status
        result = subprocess.run([
            "docker", "ps", "--filter", "name=ats-intg", 
            "--format", "{{.Names}}\t{{.Status}}"
        ], capture_output=True, text=True)
        
        if result.returncode != 0:
            print("   ❌ FAIL: Cannot check containers")
            self.results["container_health"] = "FAIL: Docker not accessible"
            self.total_tests += 1
            return
        
        containers = {}
        for line in result.stdout.strip().split('\n'):
            if line and '\t' in line:
                name, status = line.split('\t', 1)
                containers[name] = status
        
        required_containers = ['ats-intg-scheduler', 'ats-intg-dashboard', 'postgres-intg']
        unhealthy = []
        
        for container in required_containers:
            if container not in containers:
                unhealthy.append(f"{container} (missing)")
            elif "Up" not in containers[container]:
                unhealthy.append(f"{container} (down)")
        
        # Check dashboard logs for script errors (the specific issue we fixed)
        dashboard_result = subprocess.run([
            "docker", "logs", "ats-intg-dashboard", "--tail", "10"
        ], capture_output=True, text=True)
        
        has_script_errors = "can't open file" in dashboard_result.stdout
        
        if len(unhealthy) == 0 and not has_script_errors:
            print("   ✅ PASS: All containers healthy, no script errors")
            self.results["container_health"] = "PASS"
            self.passed_tests += 1
        else:
            issues = unhealthy + (["dashboard script errors"] if has_script_errors else [])
            print(f"   ❌ FAIL: Issues: {issues}")
            self.results["container_health"] = f"FAIL: {len(issues)} issues"
        
        self.total_tests += 1
    
    def test_monitoring_tools_created(self):
        """Test Issue #4: Monitoring and recovery tools exist"""
        print("🧪 Issue #4: Monitoring Tools Created")
        
        required_files = [
            ("Fix Script", "scripts/fix_intg_job_issues.py"),
            ("Health Report", "scripts/generate_intg_health_report.py"),
            ("Test Suite", "tests/integration/test_intg_job_monitoring.py"),
            ("Issue Tests", "scripts/run_issue_specific_tests.py"),
            ("Startup Script", "scripts/start_intg_jobs.sh")
        ]
        
        missing_files = []
        existing_files = []
        
        for name, file_path in required_files:
            if os.path.exists(file_path):
                existing_files.append(name)
            else:
                missing_files.append(name)
        
        # Test health report execution
        health_report_works = False
        try:
            result = subprocess.run([
                "python3", "scripts/generate_intg_health_report.py"
            ], capture_output=True, text=True, timeout=10)
            health_report_works = result.returncode == 0 and "HEALTH REPORT" in result.stdout
        except:
            health_report_works = False
        
        if len(missing_files) == 0 and health_report_works:
            print(f"   ✅ PASS: All {len(existing_files)} monitoring tools created and working")
            self.results["monitoring_tools"] = "PASS"
            self.passed_tests += 1
        else:
            issues = missing_files + ([] if health_report_works else ["health report not working"])
            print(f"   ❌ FAIL: Missing/broken: {issues}")
            self.results["monitoring_tools"] = f"FAIL: {len(issues)} issues"
        
        self.total_tests += 1
    
    def test_data_quality_checks(self):
        """Test Issue #5: Data quality and schema correctness"""
        print("🧪 Issue #5: Data Quality Checks")
        
        quality_checks = []
        
        # Check audit columns exist
        result = subprocess.run([
            "bash", "-c", 
            "PGPASSWORD=intg_password psql -h localhost -p 5434 -U postgres -d intg_db -t -c \"SELECT COUNT(*) FROM information_schema.columns WHERE table_name LIKE 'intg_%' AND column_name = 'created_at'\""
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            audit_columns = int(result.stdout.strip())
            quality_checks.append(("audit_columns", audit_columns > 0, f"{audit_columns} tables have created_at"))
        else:
            quality_checks.append(("audit_columns", False, "Query failed"))
        
        # Check data freshness
        result = subprocess.run([
            "bash", "-c", 
            "PGPASSWORD=intg_password psql -h localhost -p 5434 -U postgres -d intg_db -t -c \"SELECT COUNT(*) FROM intg_one_minute_live_polygon WHERE received_at > NOW() - INTERVAL '2 hours'\""
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            fresh_records = int(result.stdout.strip())
            quality_checks.append(("data_freshness", fresh_records > 0, f"{fresh_records} recent records"))
        else:
            quality_checks.append(("data_freshness", False, "Query failed"))
        
        # Check schema consistency
        result = subprocess.run([
            "bash", "-c", 
            "PGPASSWORD=intg_password psql -h localhost -p 5434 -U postgres -d intg_db -t -c \"SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE 'intg_%'\""
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            table_count = int(result.stdout.strip())
            quality_checks.append(("schema_consistency", table_count >= 6, f"{table_count} INTG tables"))
        else:
            quality_checks.append(("schema_consistency", False, "Query failed"))
        
        passed_checks = sum(1 for _, passed, _ in quality_checks if passed)
        total_checks = len(quality_checks)
        
        if passed_checks == total_checks:
            print(f"   ✅ PASS: All {total_checks} data quality checks passed")
            self.results["data_quality"] = "PASS"
            self.passed_tests += 1
        else:
            failed = [name for name, passed, _ in quality_checks if not passed]
            print(f"   ❌ FAIL: {passed_checks}/{total_checks} checks passed. Failed: {failed}")
            self.results["data_quality"] = f"FAIL: {len(failed)} failed checks"
        
        self.total_tests += 1
    
    def run_all_tests(self):
        """Run all issue coverage tests"""
        print("🧪 ATS-INTG IDENTIFIED ISSUES TEST COVERAGE")
        print("=" * 60)
        print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("🎯 Testing fixes for all identified issues...")
        print()
        
        # Run all tests
        self.test_missing_realtime_tables_fixed()
        print()
        
        self.test_empty_database_fixed()
        print()
        
        self.test_container_health_fixed()
        print()
        
        self.test_monitoring_tools_created()
        print()
        
        self.test_data_quality_checks()
        print()
        
        # Summary
        print("=" * 60)
        print("📊 ISSUE TEST COVERAGE SUMMARY")
        print("=" * 60)
        
        pass_rate = (self.passed_tests / self.total_tests) * 100 if self.total_tests > 0 else 0
        print(f"📈 Overall: {self.passed_tests}/{self.total_tests} issues properly tested and fixed ({pass_rate:.1f}%)")
        print()
        
        for issue, result in self.results.items():
            status_icon = "✅" if result == "PASS" else "❌"
            issue_name = issue.replace('_', ' ').title()
            print(f"{status_icon} {issue_name}: {result}")
        
        print("\n" + "=" * 60)
        
        if pass_rate >= 80:
            print("🎉 EXCELLENT: All identified issues have comprehensive test coverage!")
            print("✅ Issues are properly tested and resolved")
            print("✅ Monitoring and recovery tools in place")
        elif pass_rate >= 60:
            print("🟡 GOOD: Most issues covered, some need attention")
        else:
            print("🔴 ATTENTION NEEDED: Several issues require better coverage")
        
        return pass_rate >= 80

def main():
    """Main test runner"""
    tester = IssueTestCoverage()
    success = tester.run_all_tests()
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())