#!/usr/bin/env python3
"""
ATS Platform Cleanup Analysis Runner

This script runs comprehensive cleanup analysis for Python code and PostgreSQL tables,
generating actionable recommendations for safe cleanup based on runtime usage data.
"""

import sys
import argparse
from pathlib import Path

# Add src to Python path
sys.path.insert(0, 'src')

from src.observability.cleanup_detector import ATSCleanupDetector


def main():
    """Run cleanup analysis with database connectivity"""
    parser = argparse.ArgumentParser(description='ATS Platform Cleanup Analysis')
    parser.add_argument('--project-root', default='.', help='Project root directory')
    parser.add_argument('--output', default='ats_cleanup_report.json', help='Output report file')
    parser.add_argument('--days', type=int, default=30, help='Unused threshold in days')

    # Database configuration
    parser.add_argument('--db-host', default='localhost', help='Database host')
    parser.add_argument('--db-port', type=int, default=5432, help='Database port')
    parser.add_argument('--db-name', default='dev_db', help='Database name')
    parser.add_argument('--db-user', default='postgres', help='Database user')
    parser.add_argument('--db-password', default='dev_password', help='Database password')
    parser.add_argument('--skip-db', action='store_true', help='Skip database analysis')

    # Analysis options
    parser.add_argument('--code-only', action='store_true', help='Analyze code only')
    parser.add_argument('--db-only', action='store_true', help='Analyze database only')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')

    args = parser.parse_args()

    print("🧹 ATS Platform Cleanup Analysis")
    print("=" * 50)

    # Initialize detector
    detector = ATSCleanupDetector(args.project_root)

    # Prepare database config
    db_config = None
    if not args.skip_db and not args.code_only:
        db_config = {
            'host': args.db_host,
            'port': args.db_port,
            'database': args.db_name,
            'user': args.db_user,
            'password': args.db_password
        }

        if args.verbose:
            print(f"📊 Database config: {args.db_host}:{args.db_port}/{args.db_name}")

    try:
        # Run analysis based on options
        if args.db_only:
            print("🗄️ Running database-only analysis...")
            if not db_config:
                print("❌ Database analysis requires database configuration")
                return 1

            db_candidates = detector.analyze_database_usage(db_config, args.days)
            report = {
                'metadata': {'analysis_type': 'database_only'},
                'database_candidates': len(db_candidates),
                'candidates': db_candidates
            }
        elif args.code_only:
            print("💻 Running code-only analysis...")
            code_candidates = detector.analyze_code_usage(args.days)
            report = {
                'metadata': {'analysis_type': 'code_only'},
                'code_candidates': len(code_candidates),
                'candidates': code_candidates
            }
        else:
            print("🔍 Running comprehensive analysis...")
            report = detector.generate_cleanup_report(
                output_file=args.output,
                connection_config=db_config,
                unused_threshold_days=args.days
            )

        # Print summary results
        print("\n" + "=" * 50)
        print("📈 ANALYSIS RESULTS")
        print("=" * 50)

        if 'executive_summary' in report:
            summary = report['executive_summary']
            print(f"📊 Total candidates found: {summary['total_cleanup_candidates']}")
            print(f"💾 Estimated size reduction: {summary['estimated_size_reduction_mb']:.2f} MB")
            print(f"✅ Safe cleanup candidates: {summary['safe_cleanup_count']}")
            print(f"🔥 High priority items: {summary['high_priority_candidates']}")

            print(f"\n💡 {summary['recommendation']}")

            # Show action plan
            if 'action_plan' in report:
                action_plan = report['action_plan']
                print(f"\n🎯 IMMEDIATE ACTIONS:")
                phase1 = action_plan['phase_1_immediate']
                print(f"   Phase 1: {phase1['description']}")
                print(f"   Candidates: {len(phase1['candidates'])} items")
                print(f"   Time: {phase1['estimated_time']}")

                if phase1['candidates']:
                    print(f"   Top items: {', '.join(phase1['candidates'][:3])}...")
        else:
            # Simple analysis results
            if 'candidates' in report:
                print(f"📊 Found {len(report['candidates'])} cleanup candidates")

        print(f"\n📄 Full report saved to: {args.output}")

        # Additional recommendations
        print(f"\n🔥 NEXT STEPS:")
        print(f"1. Review the detailed report: {args.output}")
        print(f"2. Start with high-priority, low-risk candidates")
        print(f"3. Run tests after each cleanup batch")
        print(f"4. Monitor for 24-48 hours before large cleanups")
        print(f"5. Keep usage tracking running for ongoing optimization")

        return 0

    except Exception as e:
        print(f"❌ Analysis failed: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())