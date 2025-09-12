"""
ATS Automated Cleanup Detection Script

This module provides comprehensive analysis of unused Python code and PostgreSQL tables
using runtime monitoring data for safe cleanup recommendations.
"""

import json
import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Set, Any, Tuple, Optional
import subprocess
import ast
import importlib.util
from dataclasses import dataclass

from .code_usage_tracker import get_code_tracker, CodeUsageTracker
from .database_usage_tracker import get_database_tracker, DatabaseUsageTracker


@dataclass
class CleanupCandidate:
    """Represents a cleanup candidate with priority and risk assessment"""
    name: str
    type: str  # 'function', 'class', 'module', 'table'
    priority: str  # 'high', 'medium', 'low'
    risk: str  # 'low', 'medium', 'high'
    reason: str
    size_impact: int  # bytes
    dependencies: List[str]
    last_used: Optional[str]
    usage_count: int
    metadata: Dict[str, Any]


class ATSCleanupDetector:
    """
    Comprehensive cleanup detection for ATS platform using runtime monitoring data
    """

    def __init__(self, project_root: str = "/workspace"):
        self.project_root = Path(project_root)
        self.code_tracker = get_code_tracker()
        self.db_tracker = get_database_tracker()
        self.cleanup_candidates = []

    def discover_all_functions(self) -> Set[str]:
        """
        Discover all Python functions in the ATS codebase using AST parsing
        """
        all_functions = set()

        # Find all Python files in src/
        src_dir = self.project_root / "src"
        if not src_dir.exists():
            print(f"❌ Source directory not found: {src_dir}")
            return all_functions

        python_files = list(src_dir.rglob("*.py"))
        print(f"🔍 Analyzing {len(python_files)} Python files...")

        for py_file in python_files:
            try:
                with open(py_file, 'r', encoding='utf-8') as f:
                    content = f.read()

                # Parse AST to find function definitions
                tree = ast.parse(content, filename=str(py_file))

                for node in ast.walk(tree):
                    if isinstance(node, ast.FunctionDef):
                        # Get module name from file path
                        rel_path = py_file.relative_to(self.project_root)
                        module_parts = list(rel_path.parts[:-1]) + [rel_path.stem]
                        module_name = '.'.join(module_parts)

                        # Handle class methods
                        class_name = self._find_parent_class(tree, node)
                        if class_name:
                            func_name = f"{module_name}.{class_name}.{node.name}"
                        else:
                            func_name = f"{module_name}.{node.name}"

                        all_functions.add(func_name)

            except Exception as e:
                print(f"⚠️ Failed to parse {py_file}: {e}")

        print(f"✅ Discovered {len(all_functions)} functions")
        return all_functions

    def _find_parent_class(self, tree: ast.AST, func_node: ast.FunctionDef) -> Optional[str]:
        """Find the parent class of a function node"""
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                for child in node.body:
                    if child is func_node:
                        return node.name
        return None

    def analyze_code_usage(self, unused_threshold_days: int = 30) -> List[CleanupCandidate]:
        """
        Analyze Python code usage patterns and identify cleanup candidates
        """
        print("🔍 Analyzing Python code usage patterns...")

        # Get all functions in codebase
        all_functions = self.discover_all_functions()

        # Get usage statistics from tracker
        usage_stats = self.code_tracker.get_usage_stats()
        unused_analysis = self.code_tracker.get_unused_functions(all_functions)
        cleanup_candidates_raw = self.code_tracker.generate_cleanup_candidates(unused_threshold_days)

        code_candidates = []

        # Process never-used functions
        for func_name in unused_analysis['unused_functions']:
            # Estimate code size (rough approximation)
            size_estimate = len(func_name) * 50  # Rough estimate

            code_candidates.append(CleanupCandidate(
                name=func_name,
                type='function',
                priority='high',
                risk='low',
                reason='never_used',
                size_impact=size_estimate,
                dependencies=[],
                last_used=None,
                usage_count=0,
                metadata={
                    'discovery_method': 'ast_analysis',
                    'total_functions_scanned': len(all_functions)
                }
            ))

        # Process stale functions from tracker data
        for candidate in cleanup_candidates_raw:
            code_candidates.append(CleanupCandidate(
                name=candidate['function'],
                type='function',
                priority=candidate['cleanup_priority'],
                risk='medium',
                reason='stale_usage',
                size_impact=candidate['total_calls'] * 10,  # Rough estimate
                dependencies=candidate['dependencies'],
                last_used=candidate['last_used'],
                usage_count=candidate['total_calls'],
                metadata={
                    'tracking_source': 'runtime_monitoring',
                    'historical_calls': candidate['total_calls']
                }
            ))

        print(f"✅ Found {len(code_candidates)} code cleanup candidates")
        return code_candidates

    def analyze_database_usage(self, connection_config: Dict[str, str],
                             unused_threshold_days: int = 30) -> List[CleanupCandidate]:
        """
        Analyze PostgreSQL table usage patterns and identify cleanup candidates
        """
        print("🔍 Analyzing PostgreSQL table usage patterns...")

        # Get cleanup candidates from database tracker
        db_cleanup_candidates = self.db_tracker.generate_cleanup_candidates(
            connection_config, unused_threshold_days
        )

        db_candidates = []

        for candidate in db_cleanup_candidates:
            # Map cleanup priority to risk assessment
            risk_mapping = {
                'high': 'low',     # High priority cleanup = low risk
                'medium': 'medium', # Medium priority = medium risk
                'low': 'high'      # Low priority = high risk
            }

            db_candidates.append(CleanupCandidate(
                name=candidate['table'],
                type='table',
                priority=candidate['cleanup_priority'],
                risk=risk_mapping.get(candidate['cleanup_priority'], 'medium'),
                reason=candidate['reason'],
                size_impact=candidate['size_bytes'],
                dependencies=candidate['dependencies'],
                last_used=candidate.get('last_used'),
                usage_count=candidate.get('historical_access_count', 0),
                metadata={
                    'size_human': candidate['size_human'],
                    'estimated_rows': candidate['estimated_rows'],
                    'database_source': 'postgresql'
                }
            ))

        print(f"✅ Found {len(db_candidates)} database cleanup candidates")
        return db_candidates

    def calculate_cleanup_impact(self, candidates: List[CleanupCandidate]) -> Dict[str, Any]:
        """
        Calculate the overall impact of proposed cleanups
        """
        # Group by type and priority
        by_type = {}
        by_priority = {}
        total_size_impact = 0

        for candidate in candidates:
            # By type
            if candidate.type not in by_type:
                by_type[candidate.type] = []
            by_type[candidate.type].append(candidate)

            # By priority
            if candidate.priority not in by_priority:
                by_priority[candidate.priority] = []
            by_priority[candidate.priority].append(candidate)

            total_size_impact += candidate.size_impact

        # Calculate metrics
        impact_analysis = {
            'total_candidates': len(candidates),
            'total_size_impact_bytes': total_size_impact,
            'total_size_impact_mb': total_size_impact / (1024 * 1024),

            'by_type': {
                type_name: {
                    'count': len(items),
                    'size_impact': sum(c.size_impact for c in items),
                    'high_priority': len([c for c in items if c.priority == 'high']),
                    'low_risk': len([c for c in items if c.risk == 'low'])
                }
                for type_name, items in by_type.items()
            },

            'by_priority': {
                priority: {
                    'count': len(items),
                    'size_impact': sum(c.size_impact for c in items),
                    'types': list(set(c.type for c in items))
                }
                for priority, items in by_priority.items()
            },

            'safe_cleanup_estimate': len([
                c for c in candidates
                if c.priority == 'high' and c.risk in ['low', 'medium']
            ]),

            'recommended_first_pass': [
                c for c in candidates
                if c.priority == 'high' and c.risk == 'low'
            ][:10]  # Top 10 safest candidates
        }

        return impact_analysis

    def generate_cleanup_report(self, output_file: str = None,
                              connection_config: Dict[str, str] = None,
                              unused_threshold_days: int = 30) -> Dict[str, Any]:
        """
        Generate comprehensive cleanup report with recommendations
        """
        print("🚀 Generating comprehensive cleanup report...")

        # Analyze code usage
        code_candidates = self.analyze_code_usage(unused_threshold_days)

        # Analyze database usage (if config provided)
        db_candidates = []
        if connection_config:
            db_candidates = self.analyze_database_usage(connection_config, unused_threshold_days)
        else:
            print("⚠️ No database config provided, skipping database analysis")

        # Combine all candidates
        all_candidates = code_candidates + db_candidates

        # Calculate impact
        impact_analysis = self.calculate_cleanup_impact(all_candidates)

        # Generate report
        report = {
            'metadata': {
                'generated_at': datetime.now().isoformat(),
                'project_root': str(self.project_root),
                'analysis_period_days': unused_threshold_days,
                'total_candidates_found': len(all_candidates)
            },

            'executive_summary': {
                'total_cleanup_candidates': len(all_candidates),
                'estimated_size_reduction_mb': impact_analysis['total_size_impact_mb'],
                'safe_cleanup_count': impact_analysis['safe_cleanup_estimate'],
                'high_priority_candidates': len([c for c in all_candidates if c.priority == 'high']),
                'recommendation': 'Start with high-priority, low-risk candidates for safe cleanup'
            },

            'detailed_analysis': impact_analysis,

            'cleanup_candidates': {
                'high_priority_low_risk': [
                    self._candidate_to_dict(c) for c in all_candidates
                    if c.priority == 'high' and c.risk == 'low'
                ],
                'high_priority_medium_risk': [
                    self._candidate_to_dict(c) for c in all_candidates
                    if c.priority == 'high' and c.risk == 'medium'
                ],
                'medium_priority': [
                    self._candidate_to_dict(c) for c in all_candidates
                    if c.priority == 'medium'
                ],
                'all_candidates': [self._candidate_to_dict(c) for c in all_candidates]
            },

            'action_plan': self._generate_action_plan(all_candidates),

            'monitoring_data': {
                'code_usage_stats': self.code_tracker.get_usage_stats(),
                'database_usage_stats': self.db_tracker.get_database_stats() if connection_config else None
            }
        }

        # Save report if output file specified
        if output_file:
            output_path = Path(output_file)
            with open(output_path, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            print(f"✅ Report saved to: {output_path}")

        return report

    def _candidate_to_dict(self, candidate: CleanupCandidate) -> Dict[str, Any]:
        """Convert CleanupCandidate to dictionary for JSON serialization"""
        return {
            'name': candidate.name,
            'type': candidate.type,
            'priority': candidate.priority,
            'risk': candidate.risk,
            'reason': candidate.reason,
            'size_impact_bytes': candidate.size_impact,
            'size_impact_mb': candidate.size_impact / (1024 * 1024),
            'dependencies': candidate.dependencies,
            'last_used': candidate.last_used,
            'usage_count': candidate.usage_count,
            'metadata': candidate.metadata
        }

    def _generate_action_plan(self, candidates: List[CleanupCandidate]) -> Dict[str, Any]:
        """Generate step-by-step action plan for cleanup"""
        high_priority_low_risk = [c for c in candidates if c.priority == 'high' and c.risk == 'low']
        high_priority_med_risk = [c for c in candidates if c.priority == 'high' and c.risk == 'medium']

        return {
            'phase_1_immediate': {
                'description': 'Safe, immediate cleanup - no risk',
                'candidates': [c.name for c in high_priority_low_risk[:10]],
                'estimated_time': '1-2 hours',
                'prerequisites': ['Backup current state', 'Verify tests pass']
            },

            'phase_2_careful': {
                'description': 'Careful cleanup with testing',
                'candidates': [c.name for c in high_priority_med_risk[:10]],
                'estimated_time': '4-6 hours',
                'prerequisites': ['Complete phase 1', 'Run full test suite', 'Code review']
            },

            'phase_3_analysis': {
                'description': 'Deeper analysis for remaining candidates',
                'candidates_count': len(candidates) - len(high_priority_low_risk) - len(high_priority_med_risk),
                'estimated_time': '1-2 days',
                'prerequisites': ['Extended monitoring period', 'Stakeholder review']
            },

            'monitoring_recommendations': {
                'continue_tracking': 'Keep monitoring for 30+ more days',
                'seasonal_check': 'Some code may be used seasonally',
                'dependency_analysis': 'Verify no hidden dependencies exist'
            }
        }


def main():
    """Command line interface for cleanup detection"""
    parser = argparse.ArgumentParser(description='ATS Platform Cleanup Detection')
    parser.add_argument('--project-root', default='/workspace', help='Project root directory')
    parser.add_argument('--output', default='cleanup_report.json', help='Output report file')
    parser.add_argument('--days', type=int, default=30, help='Unused threshold in days')
    parser.add_argument('--db-host', default='localhost', help='Database host')
    parser.add_argument('--db-port', type=int, default=5432, help='Database port')
    parser.add_argument('--db-name', default='dev_db', help='Database name')
    parser.add_argument('--db-user', default='postgres', help='Database user')
    parser.add_argument('--db-password', default='dev_password', help='Database password')
    parser.add_argument('--skip-db', action='store_true', help='Skip database analysis')

    args = parser.parse_args()

    # Initialize detector
    detector = ATSCleanupDetector(args.project_root)

    # Prepare database config
    db_config = None
    if not args.skip_db:
        db_config = {
            'host': args.db_host,
            'port': args.db_port,
            'database': args.db_name,
            'user': args.db_user,
            'password': args.db_password
        }

    # Generate report
    try:
        report = detector.generate_cleanup_report(
            output_file=args.output,
            connection_config=db_config,
            unused_threshold_days=args.days
        )

        # Print summary
        print("\n" + "="*60)
        print("🎯 CLEANUP DETECTION SUMMARY")
        print("="*60)
        print(f"📊 Total candidates found: {report['executive_summary']['total_cleanup_candidates']}")
        print(f"💾 Estimated size reduction: {report['executive_summary']['estimated_size_reduction_mb']:.2f} MB")
        print(f"✅ Safe cleanup candidates: {report['executive_summary']['safe_cleanup_count']}")
        print(f"🔥 High priority items: {report['executive_summary']['high_priority_candidates']}")
        print(f"📄 Full report: {args.output}")
        print("\n💡 Recommendation:", report['executive_summary']['recommendation'])

        return 0

    except Exception as e:
        print(f"❌ Error generating cleanup report: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())