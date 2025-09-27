#!/usr/bin/env python3
"""
Quick test to verify the tagging system works end-to-end
"""
import asyncio
import asyncpg
import sys
import os

# Add src to path
sys.path.insert(0, 'src')

from domains.tagging.models.tag_models import CreateTagRequest, ApplyTagRequest, TagSource
from domains.tagging.repositories.tag_repository import TagRepository
from domains.tagging.services.tag_service import TagService


async def test_tagging_system():
    """Test the complete tagging system"""
    print("🧪 Testing Comprehensive Tagging System")
    print("=" * 50)
    
    # Database connection
    connection = await asyncpg.connect(
        host="localhost",
        port=4432,
        user="postgres", 
        password="intg_password",
        database="intg_db"
    )
    
    # Initialize services
    repository = TagRepository(connection)
    service = TagService(repository)
    
    print("✅ Services initialized")
    
    # Test 1: Get available tags
    print("\n📋 Test 1: Get Available Tags")
    tags = await service.get_all_tags()
    print(f"   Found {len(tags)} tags:")
    for tag in tags[:5]:  # Show first 5
        print(f"   - {tag.name} ({tag.color}) - {tag.category.name if tag.category else 'No category'}")
    
    # Test 2: Get categories
    print("\n📂 Test 2: Get Tag Categories") 
    categories = await service.get_all_categories()
    print(f"   Found {len(categories)} categories:")
    for cat in categories:
        print(f"   - {cat.name}: {cat.description}")
        
    # Test 3: Create a test tag
    print("\n🏷️ Test 3: Create Test Tag")
    test_tag_request = CreateTagRequest(
        name="Test Auto-Tag",
        description="Test tag for auto-tagging demo",
        color="#FF5722"
    )
    test_tag = await service.create_tag(test_tag_request)
    print(f"   ✅ Created test tag: {test_tag.name} (ID: {test_tag.id})")
    print("\n🔍 Test 4: Get Sample Data Quality Issues")
    query = """
    SELECT id, symbol, issue_type, description, severity, vendor_source, created_at
    FROM intg_data_quality_issues 
    LIMIT 3
    """
    issues = await connection.fetch(query)
    print(f"   Found {len(issues)} sample issues:")
    for issue in issues:
        print(f"   - Issue {issue['id']}: {issue['symbol']} - {issue['issue_type']} ({issue['severity']})")
    
    if issues:
        # Test 5: Auto-tag an issue
        print("\n🤖 Test 5: Auto-Tagging")
        sample_issue = issues[0]
        issue_data = dict(sample_issue)
        
        applied_tags = await service.auto_tag_issue(sample_issue['id'], issue_data)
        print(f"   ✅ Auto-applied {len(applied_tags)} tags to issue {sample_issue['id']}: {applied_tags}")
        
        # Test 6: Get enhanced suggestions
        print("\n💡 Test 6: Enhanced Tag Suggestions")
        suggestions = await service.get_auto_tag_suggestions_enhanced("data_quality_issues", sample_issue['id'], limit=5)
        print(f"   Found {len(suggestions)} suggestions:")
        for suggestion in suggestions:
            print(f"   - {suggestion.tag_name} ({suggestion.confidence_score:.2f}) - {suggestion.source.value}")
            if suggestion.explanation:
                print(f"     → {suggestion.explanation}")
    
    # Test 7: Auto-tagging rules
    print("\n📜 Test 7: Auto-Tagging Rules")
    auto_service = service.get_auto_tagging_service()
    rules = auto_service.get_all_rules()
    print(f"   Loaded {len(rules)} auto-tagging rules:")
    
    rule_categories = {}
    for rule in rules:
        category = rule['category']
        if category not in rule_categories:
            rule_categories[category] = []
        rule_categories[category].append(rule['tag_name'])
    
    for category, tag_names in rule_categories.items():
        print(f"   - {category}: {', '.join(set(tag_names))}")
    
    print("\n🎉 All tests completed successfully!")
    print("\n📊 Tagging System Summary:")
    print(f"   • {len(tags)} total tags available")
    print(f"   • {len(categories)} tag categories")
    print(f"   • {len(rules)} auto-tagging rules")
    print(f"   • {len(issues)} sample issues for testing")
    
if __name__ == "__main__":
    asyncio.run(test_tagging_system())