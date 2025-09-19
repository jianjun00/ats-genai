#!/usr/bin/env python3
"""
Training Data Management CLI
Comprehensive command-line interface for all training data operations.
"""

import argparse
import asyncio
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
import logging

# Import our modules with proper error handling
sys.path.append(str(Path(__file__).parent.parent.parent.parent.parent))

try:
    from domains.ml.services.training_data.dao.monthly_training_data_dao import MonthlyTrainingDataDAO
except ImportError:
    print("Warning: Could not import MonthlyTrainingDataDAO - using mock data")
    MonthlyTrainingDataDAO = None

from domains.ml.services.training_data.quality.data_quality_analyzer import AdvancedDataQualityAnalyzer
from domains.ml.services.training_data.bulk.bulk_generator import BulkTrainingDataGenerator
from domains.ml.services.training_data.search.advanced_search import TrainingDataSearchEngine, AdvancedSearchQuery, SearchFilter, SearchOperator

try:
    from domains.ml.services.training_data.monitoring.real_time_monitor import RealTimeMonitor
except ImportError:
    print("Warning: Could not import RealTimeMonitor - monitoring features disabled")
    RealTimeMonitor = None

class TrainingDataManagerCLI:
    """
    Comprehensive CLI for training data management.
    Provides unified interface for all training data operations.
    """

    def __init__(self):
        self.environment = "dev"
        self.logger = logging.getLogger(__name__)

        # Initialize components
        self.dao = None
        self.quality_analyzer = AdvancedDataQualityAnalyzer()
        self.bulk_generator = None
        self.search_engine = None
        self.monitor = None

    def setup_components(self, environment: str):
        """Initialize components for specified environment."""
        self.environment = environment

        # Initialize components with error handling
        if MonthlyTrainingDataDAO:
            try:
                # Skip DAO for demo - requires database connection
                # self.dao = MonthlyTrainingDataDAO(environment)
                pass
            except Exception as e:
                print(f"Warning: Could not initialize DAO: {e}")

        self.bulk_generator = BulkTrainingDataGenerator(environment=environment)
        self.search_engine = TrainingDataSearchEngine(environment)

        if RealTimeMonitor:
            try:
                self.monitor = RealTimeMonitor(environment)
            except Exception as e:
                print(f"Warning: Could not initialize monitor: {e}")
                self.monitor = None

    def create_parser(self) -> argparse.ArgumentParser:
        """Create the argument parser."""
        parser = argparse.ArgumentParser(
            description="🤖 Training Data Management CLI",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
Examples:
  # List recent training datasets
  %(prog)s list --limit 10 --sort-by created_at

  # Search for high-quality AAPL datasets
  %(prog)s search --symbol AAPL --quality-min 0.9

  # Generate training data for multiple symbols
  %(prog)s generate --symbols AAPL,TSLA --start-date 2024-06-01 --end-date 2024-06-30

  # Analyze data quality for a specific dataset
  %(prog)s quality --dataset-id 123

  # Start real-time monitoring
  %(prog)s monitor --start

  # Export search results
  %(prog)s export --symbol AAPL --format json --output results.json
            """
        )

        parser.add_argument('--environment', '-e',
                          choices=['dev', 'intg'],
                          default='dev',
                          help='Environment to operate on')

        parser.add_argument('--verbose', '-v',
                          action='store_true',
                          help='Enable verbose output')

        subparsers = parser.add_subparsers(dest='command', help='Available commands')

        # List command
        list_parser = subparsers.add_parser('list', help='List training datasets')
        list_parser.add_argument('--limit', type=int, default=20, help='Number of records to show')
        list_parser.add_argument('--sort-by', choices=['created_at', 'quality_score', 'file_size_mb'],
                               default='created_at', help='Sort field')
        list_parser.add_argument('--status', choices=['created', 'processing', 'completed', 'failed'],
                               help='Filter by status')

        # Search command
        search_parser = subparsers.add_parser('search', help='Search training datasets')
        search_parser.add_argument('--symbol', help='Symbol to search for')
        search_parser.add_argument('--quality-min', type=float, help='Minimum quality score')
        search_parser.add_argument('--quality-max', type=float, help='Maximum quality score')
        search_parser.add_argument('--size-min', type=float, help='Minimum file size (MB)')
        search_parser.add_argument('--size-max', type=float, help='Maximum file size (MB)')
        search_parser.add_argument('--date-from', help='Start date (YYYY-MM-DD)')
        search_parser.add_argument('--date-to', help='End date (YYYY-MM-DD)')
        search_parser.add_argument('--format', choices=['table', 'json', 'csv'], default='table',
                                 help='Output format')

        # Generate command
        generate_parser = subparsers.add_parser('generate', help='Generate training data')
        generate_parser.add_argument('--symbols', required=True, help='Comma-separated symbols')
        generate_parser.add_argument('--start-date', required=True, help='Start date (YYYY-MM-DD)')
        generate_parser.add_argument('--end-date', required=True, help='End date (YYYY-MM-DD)')
        generate_parser.add_argument('--start-offset', type=int, default=5,
                                   help='Start day offset')
        generate_parser.add_argument('--end-offset', type=int, default=2,
                                   help='End day offset')
        generate_parser.add_argument('--parallel', type=int, default=2,
                                   help='Number of parallel tasks')
        generate_parser.add_argument('--dry-run', action='store_true',
                                   help='Show what would be generated without executing')

        # Quality command
        quality_parser = subparsers.add_parser('quality', help='Analyze data quality')
        quality_parser.add_argument('--dataset-id', type=int, help='Specific dataset ID to analyze')
        quality_parser.add_argument('--symbol', help='Analyze all datasets for symbol')
        quality_parser.add_argument('--report', action='store_true',
                                  help='Generate detailed quality report')

        # Monitor command
        monitor_parser = subparsers.add_parser('monitor', help='Real-time monitoring')
        monitor_parser.add_argument('--start', action='store_true', help='Start monitoring')
        monitor_parser.add_argument('--status', action='store_true', help='Show monitoring status')
        monitor_parser.add_argument('--alerts', action='store_true', help='Show recent alerts')

        # Export command
        export_parser = subparsers.add_parser('export', help='Export training data')
        export_parser.add_argument('--symbol', help='Symbol to export')
        export_parser.add_argument('--format', choices=['json', 'csv', 'sql'], default='json',
                                 help='Export format')
        export_parser.add_argument('--output', help='Output file path')
        export_parser.add_argument('--compress', action='store_true', help='Compress output')

        # Stats command
        stats_parser = subparsers.add_parser('stats', help='Show system statistics')
        stats_parser.add_argument('--summary', action='store_true', help='Show summary stats')
        stats_parser.add_argument('--by-symbol', action='store_true', help='Group by symbol')
        stats_parser.add_argument('--by-month', action='store_true', help='Group by month')

        return parser

    async def run_command(self, args):
        """Execute the specified command."""
        self.setup_components(args.environment)

        if args.verbose:
            logging.basicConfig(level=logging.INFO)

        try:
            if args.command == 'list':
                await self.cmd_list(args)
            elif args.command == 'search':
                await self.cmd_search(args)
            elif args.command == 'generate':
                await self.cmd_generate(args)
            elif args.command == 'quality':
                await self.cmd_quality(args)
            elif args.command == 'monitor':
                await self.cmd_monitor(args)
            elif args.command == 'export':
                await self.cmd_export(args)
            elif args.command == 'stats':
                await self.cmd_stats(args)
            else:
                print("❌ No command specified. Use --help for usage information.")

        except Exception as e:
            print(f"❌ Error executing command: {e}")
            if args.verbose:
                import traceback
                traceback.print_exc()

    async def cmd_list(self, args):
        """List training datasets."""
        print(f"📋 Training Datasets ({args.environment.upper()})")
        print("=" * 50)

        try:
            # Mock records for demo
            print("💡 Using mock data (database integration available separately)")

            from dataclasses import dataclass
            from datetime import date

            @dataclass
            class MockRecord:
                id: int
                symbol: str
                year_month: date
                total_records: int
                file_size_mb: float
                data_quality_score: float
                status: str

            records = [
                MockRecord(1, "AAPL", date(2024, 6, 1), 8650, 45.2, 0.923, "completed"),
                MockRecord(2, "TSLA", date(2024, 6, 1), 12400, 67.8, 0.891, "completed"),
                MockRecord(3, "MSFT", date(2024, 6, 1), 7820, 38.9, 0.915, "completed"),
                MockRecord(4, "AAPL", date(2024, 7, 1), 9100, 48.1, 0.934, "completed"),
                MockRecord(5, "TSLA", date(2024, 7, 1), 13200, 72.3, 0.887, "processing")
            ]

            # Apply basic filtering
            if args.status:
                records = [r for r in records if r.status == args.status]

            # Apply sorting
            if args.sort_by == 'quality_score':
                records.sort(key=lambda x: x.data_quality_score, reverse=True)
            elif args.sort_by == 'file_size_mb':
                records.sort(key=lambda x: x.file_size_mb, reverse=True)
            else:  # created_at (mock with ID)
                records.sort(key=lambda x: x.id, reverse=True)

            # Apply limit
            records = records[:args.limit]

            if not records:
                print("📭 No training datasets found matching criteria")
                return

            # Display as table
            print(f"{'ID':<5} {'Symbol':<8} {'Month':<10} {'Records':<10} {'Size(MB)':<10} {'Quality':<8} {'Status':<12}")
            print("-" * 75)

            for record in records:
                status_emoji = "✅" if record.status == "completed" else "🔄" if record.status == "processing" else "❌"
                print(f"{record.id:<5} {record.symbol:<8} {record.year_month.strftime('%Y-%m'):<10} "
                     f"{record.total_records:<10,} {record.file_size_mb:<10.1f} "
                     f"{record.data_quality_score:<8.3f} {status_emoji} {record.status:<10}")

            # Calculate summary stats
            total_records = sum(r.total_records for r in records)
            total_size = sum(r.file_size_mb for r in records)
            avg_quality = sum(r.data_quality_score for r in records) / len(records) if records else 0

            print(f"\n📊 Summary:")
            print(f"  • Datasets: {len(records)}")
            print(f"  • Total Records: {total_records:,}")
            print(f"  • Total Size: {total_size:.1f} MB")
            print(f"  • Average Quality: {avg_quality:.3f}")

        except Exception as e:
            print(f"❌ Failed to list datasets: {e}")

    async def cmd_search(self, args):
        """Search training datasets."""
        print(f"🔍 Searching Training Datasets ({args.environment.upper()})")
        print("=" * 50)

        # Build search filters
        filters = []

        if args.symbol:
            filters.append(SearchFilter('symbol', SearchOperator.EQUALS, args.symbol))

        if args.quality_min is not None:
            filters.append(SearchFilter('data_quality_score', SearchOperator.GREATER_EQUAL, args.quality_min))

        if args.quality_max is not None:
            filters.append(SearchFilter('data_quality_score', SearchOperator.LESS_EQUAL, args.quality_max))

        if args.size_min is not None:
            filters.append(SearchFilter('file_size_mb', SearchOperator.GREATER_EQUAL, args.size_min))

        if args.size_max is not None:
            filters.append(SearchFilter('file_size_mb', SearchOperator.LESS_EQUAL, args.size_max))

        # Build search query
        search_query = AdvancedSearchQuery(
            filters=filters,
            limit=50
        )

        try:
            sql_query, params = self.search_engine.build_sql_query(search_query)

            print(f"🔍 Search Query:")
            for i, filter_item in enumerate(filters):
                print(f"  {i+1}. {filter_item.field} {filter_item.operator.value} {filter_item.value}")

            if args.format == 'sql':
                print(f"\n📝 Generated SQL:")
                print(sql_query)
                print(f"\n📊 Parameters: {params}")
            else:
                print(f"\n💡 Mock search results based on filters:")

                # Mock some results based on filters
                mock_results = [
                    {"symbol": "AAPL", "month": "2024-06", "quality": 0.923, "size_mb": 45.2},
                    {"symbol": "TSLA", "month": "2024-07", "quality": 0.887, "size_mb": 72.3}
                ]

                filtered_results = mock_results
                if args.symbol:
                    filtered_results = [r for r in filtered_results if r["symbol"] == args.symbol]
                if args.quality_min:
                    filtered_results = [r for r in filtered_results if r["quality"] >= args.quality_min]

                if args.format == 'json':
                    print(json.dumps(filtered_results, indent=2))
                elif args.format == 'csv':
                    print("symbol,month,quality,size_mb")
                    for r in filtered_results:
                        print(f"{r['symbol']},{r['month']},{r['quality']},{r['size_mb']}")
                else:  # table
                    print(f"\n{'Symbol':<8} {'Month':<10} {'Quality':<8} {'Size(MB)':<10}")
                    print("-" * 40)
                    for r in filtered_results:
                        print(f"{r['symbol']:<8} {r['month']:<10} {r['quality']:<8.3f} {r['size_mb']:<10.1f}")

                print(f"\n📋 Found {len(filtered_results)} matching datasets")
                print(f"📝 Generated SQL: {sql_query[:80]}...")

        except Exception as e:
            print(f"❌ Search failed: {e}")

    async def cmd_generate(self, args):
        """Generate training data."""
        symbols = [s.strip() for s in args.symbols.split(',')]
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()

        print(f"🚀 Training Data Generation ({args.environment.upper()})")
        print("=" * 50)
        print(f"📅 Date Range: {start_date} to {end_date}")
        print(f"📈 Symbols: {', '.join(symbols)}")
        print(f"⚙️ Offsets: -{args.start_offset}/+{args.end_offset} days")
        print(f"🔄 Parallel Tasks: {args.parallel}")

        # Generate tasks
        tasks = self.bulk_generator.generate_monthly_tasks(
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            start_day_offset=args.start_offset,
            end_day_offset=args.end_offset
        )

        print(f"\n📋 Generated {len(tasks)} tasks:")
        for i, task in enumerate(tasks, 1):
            collection_start = task.start_date - timedelta(days=task.start_day_offset)
            collection_end = task.end_date + timedelta(days=task.end_day_offset)
            print(f"  {i:2d}. {task.symbol} {task.start_date} → {task.end_date} "
                 f"(collect: {collection_start} → {collection_end})")

        if args.dry_run:
            print(f"\n🔍 DRY RUN - Analysis Complete:")

            # Estimate resources
            total_months = len(tasks)
            est_time_hours = total_months * 0.5 / args.parallel  # 30 min per task
            est_size_gb = total_months * 0.2  # ~200MB per month

            print(f"📊 Resource Estimates:")
            print(f"  • Estimated Time: {est_time_hours:.1f} hours")
            print(f"  • Estimated Storage: {est_size_gb:.1f} GB")
            print(f"  • Database Records: {total_months * 4} (4 timeframes per month)")
            print(f"\n💡 Use without --dry-run to execute generation")
            return

        # Execute generation
        print(f"\n🔄 Executing generation...")

        def progress_callback(completed, total, result):
            progress = completed / total * 100
            status_icon = "✅" if result.success else "❌"
            print(f"  {status_icon} [{completed:2d}/{total:2d}] {progress:5.1f}% - "
                 f"{result.task.symbol} {result.task.start_date}")

        try:
            results = self.bulk_generator.generate_bulk_training_data(tasks, progress_callback)

            # Show summary
            successful = [r for r in results if r.success]
            failed = [r for r in results if not r.success]

            print(f"\n📊 Generation Complete:")
            print(f"  ✅ Successful: {len(successful)}")
            print(f"  ❌ Failed: {len(failed)}")

            if failed:
                print(f"\n❌ Failed Tasks:")
                for result in failed[:3]:
                    print(f"  • {result.task.symbol}: {result.error_message[:60]}...")

        except Exception as e:
            print(f"❌ Generation failed: {e}")

    async def cmd_quality(self, args):
        """Analyze data quality."""
        print(f"📊 Data Quality Analysis ({args.environment.upper()})")
        print("=" * 50)

        if args.dataset_id:
            # Analyze specific dataset
            try:
                record = await self.dao.get_monthly_record(args.dataset_id)
                if not record:
                    print(f"❌ Dataset {args.dataset_id} not found")
                    return

                print(f"🔍 Analyzing dataset {args.dataset_id} ({record.symbol} {record.year_month})")

                metrics = self.quality_analyzer.analyze_monthly_data(
                    record.symbol,
                    record.year_month,
                    record.timeframe_paths
                )

                if args.report:
                    report = self.quality_analyzer.generate_quality_report(
                        metrics, record.symbol, record.year_month
                    )
                    print(report)
                else:
                    print(f"📈 Overall Quality: {metrics.overall_score:.3f}")
                    print(f"📋 Total Records: {metrics.total_records:,}")
                    print(f"⚠️ Issues: {len(metrics.issues)}")
                    print(f"💡 Warnings: {len(metrics.warnings)}")

            except Exception as e:
                print(f"❌ Quality analysis failed: {e}")

        elif args.symbol:
            print(f"🔍 Analyzing all datasets for {args.symbol}")
            # Would analyze all datasets for symbol
            print("💡 Implementation: Query all datasets for symbol and analyze each")

        else:
            print("❌ Please specify --dataset-id or --symbol")

    async def cmd_monitor(self, args):
        """Real-time monitoring commands."""
        if args.start:
            print(f"📊 Starting Real-Time Monitor ({args.environment.upper()})")
            print("=" * 50)
            print("🔄 Monitor starting... (Ctrl+C to stop)")

            try:
                await self.monitor.start_monitoring()
            except KeyboardInterrupt:
                print("\n🛑 Monitor stopped by user")
                await self.monitor.stop_monitoring()

        elif args.status:
            dashboard_data = self.monitor.get_dashboard_data()
            print(f"📊 Monitor Status ({args.environment.upper()})")
            print("=" * 50)
            print(f"🔄 Running: {dashboard_data['system_status']['running']}")
            print(f"📡 Connected Clients: {dashboard_data['system_status']['connected_clients']}")
            print(f"🚨 Total Alerts: {dashboard_data['system_status']['total_alerts']}")
            print(f"⚙️ Active Generations: {len(dashboard_data['active_generations'])}")

        elif args.alerts:
            dashboard_data = self.monitor.get_dashboard_data()
            alerts = dashboard_data['recent_alerts']

            print(f"🚨 Recent Alerts ({args.environment.upper()})")
            print("=" * 50)

            if not alerts:
                print("✅ No recent alerts")
            else:
                for alert in alerts:
                    level_icon = {"info": "ℹ️", "warning": "⚠️", "error": "❌", "critical": "🚨"}
                    icon = level_icon.get(alert['alert_level'], "❓")
                    print(f"{icon} {alert['message']} ({alert['symbol']})")

        else:
            print("❌ Please specify --start, --status, or --alerts")

    async def cmd_export(self, args):
        """Export training data."""
        print(f"📤 Exporting Training Data ({args.environment.upper()})")
        print("=" * 50)

        # Build export query
        filters = []
        if args.symbol:
            filters.append(SearchFilter('symbol', SearchOperator.EQUALS, args.symbol))

        search_query = AdvancedSearchQuery(filters=filters, limit=1000)

        try:
            export_data = self.search_engine.export_search_results(search_query, args.format)

            if args.output:
                with open(args.output, 'w') as f:
                    f.write(export_data)
                print(f"✅ Exported to {args.output}")
            else:
                print(export_data[:500] + "..." if len(export_data) > 500 else export_data)

        except Exception as e:
            print(f"❌ Export failed: {e}")

    async def cmd_stats(self, args):
        """Show system statistics."""
        print(f"📈 System Statistics ({args.environment.upper()})")
        print("=" * 50)

        try:
            # Mock statistics
            print("📊 Dataset Statistics:")
            print("  • Total Datasets: 45")
            print("  • Total Symbols: 12")
            print("  • Average Quality: 0.887")
            print("  • Total Size: 2.3 GB")
            print("  • Completed: 42 (93.3%)")
            print("  • Failed: 3 (6.7%)")

            print("\n📅 Monthly Distribution:")
            print("  • 2024-06: 15 datasets")
            print("  • 2024-07: 18 datasets")
            print("  • 2024-08: 12 datasets")

            print("\n📈 Top Symbols by Count:")
            print("  • AAPL: 8 datasets")
            print("  • TSLA: 7 datasets")
            print("  • MSFT: 6 datasets")

        except Exception as e:
            print(f"❌ Failed to get statistics: {e}")

def main():
    """Main CLI entry point."""
    cli = TrainingDataManagerCLI()
    parser = cli.create_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    # Run the command
    asyncio.run(cli.run_command(args))

if __name__ == "__main__":
    main()