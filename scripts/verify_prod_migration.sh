#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}🔍 Verifying prod_db migration results...${NC}"
echo ""

# Function to run query and display results
run_query() {
    local namespace=$1
    local query=$2
    local description=$3
    
    echo -e "${YELLOW}📊 $description${NC}"
    kubectl exec -it deployment/postgres -n $namespace -- psql -U postgres -d prod_db -c "$query"
    echo ""
}

# Check all prod tables exist and have data
echo -e "${GREEN}✅ Production Database Status:${NC}"
run_query "ats-prod" "SELECT 'prod_users' as table_name, COUNT(*) as row_count FROM prod_users
UNION ALL SELECT 'prod_products', COUNT(*) FROM prod_products  
UNION ALL SELECT 'prod_daily_prices', COUNT(*) FROM prod_daily_prices
UNION ALL SELECT 'prod_orders', COUNT(*) FROM prod_orders
UNION ALL SELECT 'prod_training_dataset', COUNT(*) FROM prod_training_dataset;" "Table row counts"

# Sample data verification
run_query "ats-prod" "SELECT * FROM prod_users LIMIT 3;" "Sample users data"
run_query "ats-prod" "SELECT * FROM prod_daily_prices LIMIT 3;" "Sample daily prices data"

# Check sequence values
run_query "ats-prod" "SELECT 
  'prod_users_id_seq' as sequence_name, last_value, is_called FROM prod_users_id_seq
UNION ALL SELECT 
  'prod_products_id_seq', last_value, is_called FROM prod_products_id_seq
UNION ALL SELECT 
  'prod_daily_prices_id_seq', last_value, is_called FROM prod_daily_prices_id_seq;" "Sequence status"

echo -e "${GREEN}🎉 Migration verification completed!${NC}"