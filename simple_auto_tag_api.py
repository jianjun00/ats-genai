#!/usr/bin/env python3
"""
Simple auto-tagging API server that works alongside analytics service
"""
import asyncio
import asyncpg
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import urllib.parse
from typing import List, Dict, Any

async def simple_auto_tag_batch(limit: int = 100) -> Dict[str, Any]:
    """Simple batch auto-tagging that works with current database setup"""
    
    conn = await asyncpg.connect(
        host="ats-intg-postgres",
        port=5432,
        user="postgres",
        password="intg_password",
        database="intg_db"
    )
    
    results = {
        "issues_processed": 0,
        "issues_tagged": 0,
        "tags_applied": 0,
        "status": "completed",
        "message": "Auto-tagging batch completed successfully"
    }
    
    # Get recent untagged issues from agent_issues
    issues = await conn.fetch("""
        SELECT ai.issue_id, ai.symbol, ai.issue_type, ai.severity, 
               COALESCE(ai.vendor, 'unknown') as vendor_source
        FROM agent_issues ai
        LEFT JOIN entity_tags et ON (
            et.entity_id::text = ai.issue_id AND 
            et.entity_type_id = (SELECT id FROM entity_types WHERE name = 'data_quality_issues')
        )
        WHERE et.id IS NULL
        AND ai.created_at > NOW() - INTERVAL '7 days'
        LIMIT $1
    """, limit)
    
    for issue in issues:
        issue_data = dict(issue)
        applied_tags = await apply_simple_auto_tags(conn, issue['issue_id'], issue_data)
        
        results["issues_processed"] += 1
        if applied_tags:
            results["issues_tagged"] += 1
            results["tags_applied"] += len(applied_tags)
            
    results["message"] = f"Processed {results['issues_processed']} issues, tagged {results['issues_tagged']} issues with {results['tags_applied']} total tags"
            
    return results

async def apply_simple_auto_tags(conn, issue_id: str, issue_data: Dict[str, Any]) -> List[str]:
    """Apply simple auto-tagging rules"""
    
    applied_tags = []
    rules = []
    
    # Severity-based rules
    severity = issue_data.get('severity', '').lower()
    if severity in ['critical', 'high', 'medium', 'low']:
        rules.append(severity.title())
        
    # Vendor source rules
    vendor = issue_data.get('vendor_source', '').lower()
    vendor_map = {'polygon': 'Polygon', 'tiingo': 'Tiingo', 'eodhd': 'EODHD', 'firstrate': 'FirstRate'}
    if vendor in vendor_map:
        rules.append(vendor_map[vendor])
        
    # Issue type rules
    issue_type = issue_data.get('issue_type', '').lower()
    if 'missing' in issue_type or 'gap' in issue_type:
        rules.append('Data Gap')
    elif 'price' in issue_type and 'anomaly' in issue_type:
        rules.append('Price Anomaly')
    elif 'volume' in issue_type:
        rules.append('Volume Spike')
        
    # Apply tags
    for tag_name in rules:
        # Find tag
        tag_result = await conn.fetchrow("SELECT id FROM tags WHERE name = $1", tag_name)
        if tag_result:
            # Apply tag
            await conn.execute("""
                INSERT INTO entity_tags (entity_type_id, entity_id, tag_id, source, confidence_score, metadata)
                VALUES (
                    (SELECT id FROM entity_types WHERE name = 'data_quality_issues'),
                    $1, $2, 'auto', 0.9, '{}'
                )
                ON CONFLICT (entity_type_id, entity_id, tag_id) DO NOTHING
            """, hash(issue_id) % 2147483647, tag_result['id'])
            
            applied_tags.append(tag_name)
    return applied_tags

class AutoTaggingHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path == '/auto-tag-batch':
            self._handle_auto_tag_batch()
        else:
            self._send_404()
    
    def do_GET(self):
        if self.path == '/auto-tag-batch':
            self._handle_auto_tag_batch()
        else:
            self._send_404()
    
    def _handle_auto_tag_batch(self):
        # Run auto-tagging
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        results = loop.run_until_complete(simple_auto_tag_batch(limit=100))
        loop.close()
        
        # Send response
        response = json.dumps(results, indent=2)
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(response.encode())
        
    def _send_404(self):
        self.send_response(404)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        error_response = {"error": "Not found", "available_endpoints": ["/auto-tag-batch"]}
        self.wfile.write(json.dumps(error_response).encode())
    
    def log_message(self, format, *args):
        pass  # Suppress default logging

def main():
    port = 4005
    server = ThreadingHTTPServer(('0.0.0.0', port), AutoTaggingHandler)
    print(f"🤖 Auto-tagging API server started at http://0.0.0.0:{port}")
    print(f"🔗 Endpoint: http://localhost:{port}/auto-tag-batch")
    server.serve_forever()

if __name__ == "__main__":
    main()