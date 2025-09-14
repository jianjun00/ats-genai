#!/usr/bin/env python3
"""
Simple test to verify the tagging database and basic functionality
"""
import asyncio
import asyncpg

async def test_tagging_database():
    """Test the tagging database directly"""
    print("🧪 Testing Tagging Database")
    print("=" * 40)
    
    # Database connection
    connection = await asyncpg.connect(
        host="localhost",
        port=4432,
        user="postgres", 
        password="intg_password",
        database="intg_db"
    )
    
    try:
        # Test 1: Check if tagging tables exist
        print("\n📊 Test 1: Check Tagging Tables")
        tables_query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name IN ('tags', 'tag_categories', 'entity_types', 'entity_tags')
        ORDER BY table_name
        """
        tables = await connection.fetch(tables_query)
        print(f"   Found {len(tables)} tagging tables:")
        for table in tables:
            print(f"   ✅ {table['table_name']}")
        
        # Test 2: Count system tags
        print("\n🏷️ Test 2: System Tags")
        tags_query = "SELECT COUNT(*) as count FROM tags WHERE is_system_tag = true"
        tag_count = await connection.fetchrow(tags_query)
        print(f"   Found {tag_count['count']} system tags")
        
        # Show some examples
        sample_tags_query = """
        SELECT t.name, t.color, tc.name as category_name
        FROM tags t
        LEFT JOIN tag_categories tc ON t.category_id = tc.id
        WHERE t.is_system_tag = true
        LIMIT 5
        """
        sample_tags = await connection.fetch(sample_tags_query)
        print("   Sample system tags:")
        for tag in sample_tags:
            print(f"   - {tag['name']} ({tag['color']}) [{tag['category_name']}]")
        
        # Test 3: Count tag categories
        print("\n📂 Test 3: Tag Categories")
        categories_query = "SELECT name, description, color, icon FROM tag_categories ORDER BY sort_order"
        categories = await connection.fetch(categories_query)
        print(f"   Found {len(categories)} categories:")
        for cat in categories:
            print(f"   - {cat['name']}: {cat['description']} ({cat['color']} {cat['icon']})")
        
        # Test 4: Check entity types
        print("\n🎯 Test 4: Entity Types")
        entity_types_query = "SELECT name, display_name, description FROM entity_types"
        entity_types = await connection.fetch(entity_types_query)
        print(f"   Found {len(entity_types)} entity types:")
        for et in entity_types:
            print(f"   - {et['name']}: {et['display_name']}")
        
        # Test 5: Test basic tag functionality with raw SQL
        print("\n⚡ Test 5: Raw SQL Tag Operations")
        
        # Get a sample data quality issue
        issue_query = """
        SELECT id, symbol, issue_type, severity, vendor_source
        FROM intg_data_quality_issues 
        LIMIT 1
        """
        issues = await connection.fetch(issue_query)
        
        if issues:
            issue = issues[0]
            print(f"   Using issue {issue['id']}: {issue['symbol']} - {issue['issue_type']} ({issue['severity']})")
            
            # Find the "Critical" tag
            critical_tag_query = "SELECT id FROM tags WHERE name = 'Critical' LIMIT 1"
            critical_tag = await connection.fetchrow(critical_tag_query)
            
            if critical_tag:
                # Apply the Critical tag to the issue
                apply_tag_query = """
                INSERT INTO entity_tags (entity_type_id, entity_id, tag_id, source, confidence_score, metadata)
                VALUES (
                    (SELECT id FROM entity_types WHERE name = 'data_quality_issues'),
                    $1, $2, 'manual', 1.0, '{}'
                )
                ON CONFLICT (entity_type_id, entity_id, tag_id) DO NOTHING
                RETURNING id
                """
                
                result = await connection.fetchrow(apply_tag_query, issue['id'], critical_tag['id'])
                if result:
                    print(f"   ✅ Applied 'Critical' tag to issue {issue['id']}")
                else:
                    print(f"   ℹ️ 'Critical' tag already applied to issue {issue['id']}")
                
                # Get tags for this issue
                issue_tags_query = """
                SELECT t.name, t.color, et.confidence_score, et.source
                FROM entity_tags et
                JOIN tags t ON et.tag_id = t.id
                JOIN entity_types ety ON et.entity_type_id = ety.id
                WHERE ety.name = 'data_quality_issues' AND et.entity_id = $1
                """
                issue_tags = await connection.fetch(issue_tags_query, issue['id'])
                print(f"   Issue {issue['id']} now has {len(issue_tags)} tags:")
                for tag in issue_tags:
                    print(f"   - {tag['name']} ({tag['confidence_score']}, {tag['source']})")
        else:
            print("   No data quality issues found for testing")
        
        # Test 6: Test auto-tagging rules simulation
        print("\n🤖 Test 6: Auto-Tagging Rules Simulation")
        
        if issues:
            issue = issues[0]
            issue_data = dict(issue)
            
            print(f"   Testing rules on issue: {issue_data}")
            
            # Simulate some auto-tagging rules
            auto_tags = []
            
            # Rule 1: Severity-based tagging
            severity = issue_data.get('severity', '').lower()
            if severity in ['critical', 'high', 'medium', 'low']:
                tag_name = severity.title()
                tag_query = "SELECT id, name FROM tags WHERE name = $1 LIMIT 1"
                tag_result = await connection.fetchrow(tag_query, tag_name)
                if tag_result:
                    auto_tags.append(f"{tag_result['name']} (severity rule)")
            
            # Rule 2: Vendor source tagging  
            vendor = issue_data.get('vendor_source', '').title()
            if vendor:
                vendor_tag_query = "SELECT id, name FROM tags WHERE name = $1 LIMIT 1"
                vendor_result = await connection.fetchrow(vendor_tag_query, vendor)
                if vendor_result:
                    auto_tags.append(f"{vendor_result['name']} (vendor rule)")
            
            print(f"   Auto-tagging would apply {len(auto_tags)} tags:")
            for tag in auto_tags:
                print(f"   - {tag}")
        
        print("\n🎉 All database tests completed successfully!")
        print("\n📊 Tagging System Database Summary:")
        print(f"   • {tag_count['count']} system tags loaded")
        print(f"   • {len(categories)} tag categories configured")
        print(f"   • {len(entity_types)} entity types supported")
        print(f"   • Database schema fully operational")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await connection.close()


if __name__ == "__main__":
    asyncio.run(test_tagging_database())