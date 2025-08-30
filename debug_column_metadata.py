#!/usr/bin/env python3
"""Debug column metadata function"""

import sys
sys.path.insert(0, 'src')

from core.database.connection_manager import get_raw_connection
from psycopg2.extras import RealDictCursor

def test_column_metadata():
    try:
        with get_raw_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns 
                    WHERE table_name = %s AND column_name = %s
                """, ('dev_daily_prices_tiingo', 'volume'))
                
                result = cursor.fetchone()
                print(f"Raw result: {result}")
                print(f"Result type: {type(result)}")
                
                if result:
                    print(f"Column metadata: {dict(result)}")
                    return {
                        'column_name': result['column_name'],
                        'data_type': result['data_type'], 
                        'is_nullable': result['is_nullable']
                    }
                else:
                    print("No result found")
                    return None
                    
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    result = test_column_metadata()
    print(f"Final result: {result}")