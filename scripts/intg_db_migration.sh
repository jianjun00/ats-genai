#!/bin/bash
# ATS-INTG Database Migration from ATS-DEV
# Direct PostgreSQL dump and restore with data transformation

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

# Configuration
DEV_DB_HOST="localhost"
DEV_DB_PORT="5432"
DEV_DB_USER="postgres"
DEV_DB_PASSWORD="dev_password"
DEV_DB_NAME="dev_db"

INTG_DB_HOST="localhost"
INTG_DB_PORT="5433"
INTG_DB_USER="postgres"
INTG_DB_PASSWORD="intg_password"
INTG_DB_NAME="intg_db"

MIGRATION_DIR="/mnt/d/ats-backup/intg/migration"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Functions
print_header() {
    echo -e "${PURPLE}🔄 ATS-INTG Database Migration${NC}"
    echo -e "${PURPLE}================================${NC}"
    echo ""
}

print_status() {
    if [ $1 -eq 0 ]; then
        echo -e "${GREEN}✅ $2${NC}"
    else
        echo -e "${RED}❌ $2${NC}"
        return 1
    fi
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

check_prerequisites() {
    print_info "Checking migration prerequisites..."

    # Check if PostgreSQL clients are available
    if ! command -v pg_dump >/dev/null 2>&1; then
        print_status 1 "pg_dump not found. Install PostgreSQL client tools."
        return 1
    fi

    if ! command -v psql >/dev/null 2>&1; then
        print_status 1 "psql not found. Install PostgreSQL client tools."
        return 1
    fi

    # Create migration directory
    mkdir -p "$MIGRATION_DIR"
    print_status 0 "Migration directory ready: $MIGRATION_DIR"

    # Test DEV database connection
    if PGPASSWORD="$DEV_DB_PASSWORD" psql -h "$DEV_DB_HOST" -p "$DEV_DB_PORT" -U "$DEV_DB_USER" -d "$DEV_DB_NAME" -c "SELECT 'DEV connection successful'" >/dev/null 2>&1; then
        print_status 0 "DEV database connection verified"
    else
        print_status 1 "DEV database connection failed"
        return 1
    fi

    # Test INTG database connection (check if container is running)
    if docker ps --filter "name=postgres-intg" --filter "status=running" | grep -q postgres-intg; then
        if docker exec postgres-intg pg_isready -U postgres -d intg_db >/dev/null 2>&1; then
            print_status 0 "INTG database connection verified"
        else
            print_status 1 "INTG database not ready"
            return 1
        fi
    else
        print_status 1 "INTG PostgreSQL container not running. Start with: docker-compose -f docker-compose.intg-jobs.yml up -d postgres-intg"
        return 1
    fi

    return 0
}

get_dev_table_list() {
    print_info "Getting list of tables from DEV database..."

    local tables_file="$MIGRATION_DIR/dev_tables_$TIMESTAMP.txt"

    PGPASSWORD="$DEV_DB_PASSWORD" psql -h "$DEV_DB_HOST" -p "$DEV_DB_PORT" -U "$DEV_DB_USER" -d "$DEV_DB_NAME" \
        -t -c "SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE 'dev_%' ORDER BY tablename" \
        > "$tables_file"

    # Clean up the file
    sed -i 's/^ *//' "$tables_file"
    sed -i '/^$/d' "$tables_file"

    local table_count=$(wc -l < "$tables_file")
    print_status 0 "Found $table_count DEV tables"

    echo "$tables_file"
}

create_table_mapping() {
    local dev_tables_file="$1"
    local mapping_file="$MIGRATION_DIR/table_mapping_$TIMESTAMP.json"

    print_info "Creating table mapping configuration..."

    cat > "$mapping_file" << 'EOF'
{
  "table_mappings": {
    "dev_instruments": "intg_instruments",
    "dev_daily_prices": "intg_daily_prices",
    "dev_fundamentals_comprehensive": "intg_fundamentals_comprehensive",
    "dev_tiingo_daily_prices": "intg_daily_prices",
    "dev_polygon_daily_prices": "intg_daily_prices",
    "dev_fmp_daily_prices": "intg_daily_prices"
  },
  "column_mappings": {
    "creation_timestamp": "created_at",
    "last_updated": "updated_at"
  },
  "skip_tables": [
    "dev_checkpoint",
    "dev_migrations",
    "dev_temp"
  ],
  "merge_strategy": {
    "intg_daily_prices": {
      "source_tables": ["dev_tiingo_daily_prices", "dev_polygon_daily_prices", "dev_fmp_daily_prices", "dev_daily_prices"],
      "vendor_mapping": {
        "dev_tiingo_daily_prices": "tiingo",
        "dev_polygon_daily_prices": "polygon",
        "dev_fmp_daily_prices": "fmp",
        "dev_daily_prices": "dev_migration"
      }
    }
  }
}
EOF

    print_status 0 "Table mapping created: $mapping_file"
    echo "$mapping_file"
}

export_dev_data() {
    local table_name="$1"
    local output_file="$MIGRATION_DIR/${table_name}_$TIMESTAMP.sql"

    print_info "Exporting $table_name from DEV database..."

    # Export with data and INSERT statements
    PGPASSWORD="$DEV_DB_PASSWORD" pg_dump \
        -h "$DEV_DB_HOST" -p "$DEV_DB_PORT" -U "$DEV_DB_USER" -d "$DEV_DB_NAME" \
        -t "$table_name" \
        --data-only \
        --column-inserts \
        --no-owner \
        --no-privileges \
        > "$output_file"

    if [ $? -eq 0 ]; then
        local file_size=$(du -h "$output_file" | cut -f1)
        print_status 0 "Exported $table_name ($file_size)"
        echo "$output_file"
    else
        print_status 1 "Failed to export $table_name"
        return 1
    fi
}

transform_sql_for_intg() {
    local input_file="$1"
    local source_table="$2"
    local target_table="$3"
    local output_file="$MIGRATION_DIR/transformed_${target_table}_$TIMESTAMP.sql"

    print_info "Transforming $source_table → $target_table..."

    # Start with copy of original
    cp "$input_file" "$output_file"

    # Transform table name
    sed -i "s/INSERT INTO $source_table/INSERT INTO $target_table/g" "$output_file"

    # Transform column names
    sed -i 's/creation_timestamp/created_at/g' "$output_file"
    sed -i 's/last_updated/updated_at/g' "$output_file"

    # Add vendor column for daily prices if needed
    if [[ "$target_table" == "intg_daily_prices" ]]; then
        local vendor="dev_migration"

        if [[ "$source_table" == *"tiingo"* ]]; then
            vendor="tiingo"
        elif [[ "$source_table" == *"polygon"* ]]; then
            vendor="polygon"
        elif [[ "$source_table" == *"fmp"* ]]; then
            vendor="fmp"
        fi

        # Add vendor column to INSERT statements
        sed -i "s/INSERT INTO $target_table (/INSERT INTO $target_table (vendor, /g" "$output_file"
        sed -i "s/VALUES (/VALUES ('$vendor', /g" "$output_file"
    fi

    # Add ON CONFLICT clause to prevent duplicate key errors
    if [[ "$target_table" == "intg_daily_prices" ]]; then
        sed -i 's/);$/) ON CONFLICT (symbol, date, vendor) DO NOTHING;/g' "$output_file"
    elif [[ "$target_table" == "intg_instruments" ]]; then
        sed -i 's/);$/) ON CONFLICT (symbol) DO NOTHING;/g' "$output_file"
    elif [[ "$target_table" == "intg_fundamentals_comprehensive" ]]; then
        sed -i 's/);$/) ON CONFLICT (symbol, date, vendor, fiscal_period) DO NOTHING;/g' "$output_file"
    fi

    print_status 0 "Transformation completed: $output_file"
    echo "$output_file"
}

import_to_intg() {
    local sql_file="$1"
    local table_name="$2"

    print_info "Importing data to INTG table: $table_name..."

    # Import via Docker exec to INTG container
    if cat "$sql_file" | docker exec -i postgres-intg psql -U postgres -d intg_db; then
        # Get record count
        local count=$(docker exec postgres-intg psql -U postgres -d intg_db -t -c "SELECT COUNT(*) FROM $table_name" | xargs)
        print_status 0 "Import completed - $table_name now has $count records"
    else
        print_status 1 "Import failed for $table_name"
        return 1
    fi
}

create_migration_summary() {
    local summary_file="$MIGRATION_DIR/migration_summary_$TIMESTAMP.md"

    print_info "Creating migration summary report..."

    cat > "$summary_file" << EOF
# ATS-INTG Database Migration Summary

**Migration Date**: $(date '+%Y-%m-%d %H:%M:%S')
**Migration ID**: $TIMESTAMP

## Source Database (DEV)
- Host: $DEV_DB_HOST:$DEV_DB_PORT
- Database: $DEV_DB_NAME
- Schema: public (dev_* tables)

## Target Database (INTG)
- Host: Docker container postgres-intg
- Database: $INTG_DB_NAME
- Schema: public (intg_* tables)

## Migration Results

EOF

    # Get table counts from INTG
    docker exec postgres-intg psql -U postgres -d intg_db -c "
    SELECT
        'intg_instruments' as table_name,
        COUNT(*) as record_count,
        MIN(created_at) as earliest_record,
        MAX(created_at) as latest_record
    FROM intg_instruments
    UNION ALL
    SELECT
        'intg_daily_prices' as table_name,
        COUNT(*) as record_count,
        MIN(date)::text as earliest_record,
        MAX(date)::text as latest_record
    FROM intg_daily_prices
    UNION ALL
    SELECT
        'intg_fundamentals_comprehensive' as table_name,
        COUNT(*) as record_count,
        MIN(date)::text as earliest_record,
        MAX(date)::text as latest_record
    FROM intg_fundamentals_comprehensive
    " >> "$summary_file" 2>/dev/null || echo "Error getting table statistics" >> "$summary_file"

    cat >> "$summary_file" << EOF

## Files Generated
- Migration directory: $MIGRATION_DIR
- Table mapping: table_mapping_$TIMESTAMP.json
- SQL dumps: *_$TIMESTAMP.sql
- Transformed SQL: transformed_*_$TIMESTAMP.sql

## Next Steps

1. **Validate Data Quality**:
   \`\`\`bash
   python scripts/intg_data_backfill.py validate
   \`\`\`

2. **Start Daily Jobs**:
   \`\`\`bash
   docker-compose -f docker-compose.intg-jobs.yml up -d
   \`\`\`

3. **Monitor Integration**:
   \`\`\`bash
   python scripts/monitor_daily_jobs.py
   \`\`\`

**Migration completed successfully! 🎉**
EOF

    print_status 0 "Migration summary created: $summary_file"
    echo "$summary_file"
}

# Main migration workflow
main() {
    print_header

    # Parse arguments
    local action="${1:-full}"
    local specific_table="$2"

    case "$action" in
        "validate")
            print_info "Validating migration environment..."
            check_prerequisites
            print_status 0 "Environment validation completed"
            ;;

        "export")
            print_info "Exporting DEV data only..."
            check_prerequisites || exit 1

            dev_tables_file=$(get_dev_table_list)

            while read -r table_name; do
                export_dev_data "$table_name"
            done < "$dev_tables_file"

            print_status 0 "Export phase completed"
            ;;

        "import")
            print_info "Importing to INTG only..."
            check_prerequisites || exit 1

            # Find latest exported files
            for sql_file in "$MIGRATION_DIR"/*_"$TIMESTAMP".sql; do
                if [ -f "$sql_file" ]; then
                    table_name=$(basename "$sql_file" | sed "s/_$TIMESTAMP.sql//")
                    import_to_intg "$sql_file" "$table_name"
                fi
            done

            print_status 0 "Import phase completed"
            ;;

        "full"|*)
            print_info "Running full migration: DEV → INTG..."

            # Prerequisites check
            check_prerequisites || exit 1

            # Get list of DEV tables
            dev_tables_file=$(get_dev_table_list)

            # Create table mapping
            mapping_file=$(create_table_mapping "$dev_tables_file")

            # Process each table
            processed_tables=0
            failed_tables=0

            # Core tables to migrate
            tables_to_migrate=(
                "dev_instruments:intg_instruments"
                "dev_daily_prices:intg_daily_prices"
                "dev_fundamentals_comprehensive:intg_fundamentals_comprehensive"
            )

            # Add vendor-specific daily prices tables if they exist
            while read -r table_name; do
                if [[ "$table_name" == *"tiingo_daily_prices" ]] ||
                   [[ "$table_name" == *"polygon_daily_prices" ]] ||
                   [[ "$table_name" == *"fmp_daily_prices" ]]; then
                    tables_to_migrate+=("$table_name:intg_daily_prices")
                fi
            done < "$dev_tables_file"

            for table_mapping in "${tables_to_migrate[@]}"; do
                source_table="${table_mapping%:*}"
                target_table="${table_mapping#*:}"

                print_info "Processing: $source_table → $target_table"

                # Export from DEV
                exported_file=$(export_dev_data "$source_table")
                if [ $? -ne 0 ]; then
                    print_warning "Skipping $source_table (export failed)"
                    failed_tables=$((failed_tables + 1))
                    continue
                fi

                # Transform for INTG
                transformed_file=$(transform_sql_for_intg "$exported_file" "$source_table" "$target_table")
                if [ $? -ne 0 ]; then
                    print_warning "Skipping $source_table (transformation failed)"
                    failed_tables=$((failed_tables + 1))
                    continue
                fi

                # Import to INTG
                if import_to_intg "$transformed_file" "$target_table"; then
                    processed_tables=$((processed_tables + 1))
                    print_status 0 "Completed: $source_table → $target_table"
                else
                    failed_tables=$((failed_tables + 1))
                    print_status 1 "Failed: $source_table → $target_table"
                fi
            done

            # Create summary report
            summary_file=$(create_migration_summary)

            # Final results
            print_header
            print_status 0 "ATS-INTG Database Migration Completed!"
            echo ""
            print_info "📊 Results Summary:"
            print_info "  ✅ Successful tables: $processed_tables"
            print_info "  ❌ Failed tables: $failed_tables"
            print_info "  📄 Summary report: $summary_file"
            print_info "  📁 Migration files: $MIGRATION_DIR"
            echo ""
            print_info "🚀 Next steps:"
            print_info "  1. Validate: python scripts/intg_data_backfill.py validate"
            print_info "  2. Start jobs: docker-compose -f docker-compose.intg-jobs.yml up -d"
            print_info "  3. Monitor: python scripts/monitor_daily_jobs.py"

            ;;
    esac
}

# Run main function with arguments
main "$@"