"""
Repository for tag database operations
"""
import asyncpg
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime
import logging

from src.domains.tagging.models.tag_models import (
    Tag, TagCategory, EntityType, EntityTag, TagUsageStats,
    TagFilter, TagSuggestion, TagAnalytics, TaggedIssue,
    CreateTagRequest, ApplyTagRequest, BulkTagRequest, TagSource
)

logger = logging.getLogger(__name__)


class TagRepository:
    def __init__(self, db_pool: asyncpg.Pool):
        self.db_pool = db_pool
    
    async def create_tag(self, request: CreateTagRequest, user_id: Optional[str] = None) -> Tag:
        """Create a new tag"""
        query = """
        INSERT INTO tags (name, slug, description, category_id, color, metadata, is_system_tag)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING id, name, slug, description, color, category_id, 
                  created_at, updated_at, usage_count, is_system_tag, is_active, metadata
        """
        
        # Generate slug from name
        slug = self._generate_slug(request.name)
        
        async with self.db_pool.acquire() as conn:
            row = await conn.fetchrow(
                query,
                request.name,
                slug,
                request.description,
                request.category_id,
                request.color or '#6c757d',
                request.metadata or {},
                False  # User-created tags are not system tags
            )
            
            return self._row_to_tag(row)
    
    async def get_tag_by_id(self, tag_id: int) -> Optional[Tag]:
        """Get a tag by ID"""
        query = """
        SELECT t.id, t.name, t.slug, t.description, t.color, t.category_id,
               t.created_at, t.updated_at, t.usage_count, t.is_system_tag, 
               t.is_active, t.metadata,
               tc.name as category_name, tc.slug as category_slug,
               tc.color as category_color, tc.icon as category_icon
        FROM tags t
        LEFT JOIN tag_categories tc ON t.category_id = tc.id
        WHERE t.id = $1 AND t.is_active = true
        """
        
        async with self.db_pool.acquire() as conn:
            row = await conn.fetchrow(query, tag_id)
            return self._row_to_tag(row) if row else None
    
    async def get_tags(self, 
                      search: Optional[str] = None,
                      category_id: Optional[int] = None,
                      is_system_tag: Optional[bool] = None,
                      limit: int = 50,
                      offset: int = 0) -> List[Tag]:
        """Get tags with filtering and pagination"""
        conditions = ["t.is_active = true"]
        params = []
        param_count = 0
        
        if search:
            param_count += 1
            conditions.append(f"(t.name ILIKE ${param_count} OR t.description ILIKE ${param_count})")
            params.append(f"%{search}%")
        
        if category_id:
            param_count += 1
            conditions.append(f"t.category_id = ${param_count}")
            params.append(category_id)
            
        if is_system_tag is not None:
            param_count += 1
            conditions.append(f"t.is_system_tag = ${param_count}")
            params.append(is_system_tag)
        
        where_clause = " AND ".join(conditions)
        
        query = f"""
        SELECT t.id, t.name, t.slug, t.description, t.color, t.category_id,
               t.created_at, t.updated_at, t.usage_count, t.is_system_tag, 
               t.is_active, t.metadata,
               tc.name as category_name, tc.slug as category_slug,
               tc.color as category_color, tc.icon as category_icon
        FROM tags t
        LEFT JOIN tag_categories tc ON t.category_id = tc.id
        WHERE {where_clause}
        ORDER BY t.usage_count DESC, t.name ASC
        LIMIT ${param_count + 1} OFFSET ${param_count + 2}
        """
        
        params.extend([limit, offset])
        
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
            return [self._row_to_tag(row) for row in rows]
    
    async def get_popular_tags(self, entity_type: str, days: int = 30, limit: int = 20) -> List[Tag]:
        """Get most popular tags for an entity type"""
        query = """
        SELECT t.id, t.name, t.slug, t.description, t.color, t.category_id,
               t.created_at, t.updated_at, t.usage_count, t.is_system_tag, 
               t.is_active, t.metadata,
               tc.name as category_name, tc.slug as category_slug,
               tc.color as category_color, tc.icon as category_icon,
               COUNT(et.id) as recent_usage
        FROM tags t
        LEFT JOIN tag_categories tc ON t.category_id = tc.id
        LEFT JOIN entity_tags et ON t.id = et.tag_id
        LEFT JOIN entity_types ety ON et.entity_type_id = ety.id
        WHERE t.is_active = true
        AND (ety.name = $1 OR ety.name IS NULL)
        AND (et.tagged_at >= CURRENT_DATE - INTERVAL '%s days' OR et.tagged_at IS NULL)
        GROUP BY t.id, t.name, t.slug, t.description, t.color, t.category_id,
                 t.created_at, t.updated_at, t.usage_count, t.is_system_tag, 
                 t.is_active, t.metadata, tc.name, tc.slug, tc.color, tc.icon
        ORDER BY recent_usage DESC, t.usage_count DESC
        LIMIT $2
        """ % days
        
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch(query, entity_type, limit)
            return [self._row_to_tag(row) for row in rows]
    
    async def get_tag_categories(self) -> List[TagCategory]:
        """Get all tag categories"""
        query = """
        SELECT id, name, slug, description, color, icon, parent_id, 
               sort_order, created_at, updated_at
        FROM tag_categories
        ORDER BY sort_order ASC, name ASC
        """
        
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch(query)
            return [self._row_to_tag_category(row) for row in rows]
    
    async def get_entity_types(self) -> List[EntityType]:
        """Get all entity types"""
        query = """
        SELECT id, name, table_name, display_name, description, is_active, created_at
        FROM entity_types
        WHERE is_active = true
        ORDER BY display_name ASC
        """
        
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch(query)
            return [self._row_to_entity_type(row) for row in rows]
    
    async def apply_tag(self, request: ApplyTagRequest, user_id: Optional[str] = None) -> int:
        """Apply a tag to an entity"""
        # Get entity_type_id
        entity_type_id = await self._get_entity_type_id(request.entity_type)
        if not entity_type_id:
            raise ValueError(f"Unknown entity type: {request.entity_type}")
        
        query = """
        INSERT INTO entity_tags (entity_type_id, entity_id, tag_id, tagged_by_user_id, 
                               confidence_score, source, metadata)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (entity_type_id, entity_id, tag_id) 
        DO UPDATE SET
            confidence_score = $5,
            source = $6,
            metadata = $7,
            tagged_at = CURRENT_TIMESTAMP
        RETURNING id
        """
        
        async with self.db_pool.acquire() as conn:
            row = await conn.fetchrow(
                query,
                entity_type_id,
                request.entity_id,
                request.tag_id,
                user_id,
                request.confidence_score,
                request.source.value,
                request.metadata or {}
            )
            return row['id']
    
    async def remove_tag(self, entity_type: str, entity_id: int, tag_id: int) -> bool:
        """Remove a tag from an entity"""
        entity_type_id = await self._get_entity_type_id(entity_type)
        if not entity_type_id:
            return False
        
        query = """
        DELETE FROM entity_tags
        WHERE entity_type_id = $1 AND entity_id = $2 AND tag_id = $3
        """
        
        async with self.db_pool.acquire() as conn:
            result = await conn.execute(query, entity_type_id, entity_id, tag_id)
            return result.split()[-1] == '1'  # Check if one row was deleted
    
    async def get_entity_tags(self, entity_type: str, entity_id: int) -> List[Tag]:
        """Get all tags for a specific entity"""
        query = """
        SELECT tag_id, tag_name, tag_slug, tag_color, category_name, 
               tagged_at, confidence_score, source
        FROM get_entity_tags($1, $2)
        """
        
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch(query, entity_type, entity_id)
            tags = []
            for row in rows:
                tag = Tag(
                    id=row['tag_id'],
                    name=row['tag_name'],
                    slug=row['tag_slug'],
                    description=None,
                    color=row['tag_color'],
                    category_id=None,
                    category=None,
                    created_at=datetime.now(),
                    updated_at=datetime.now(),
                    usage_count=0,
                    is_system_tag=False,
                    is_active=True,
                    metadata={}
                )
                tags.append(tag)
            return tags
    
    async def filter_entities_by_tags(self, filter_request: TagFilter) -> List[int]:
        """Filter entities by tags and return entity IDs"""
        if not filter_request.tag_ids:
            return []
        
        # Use the stored function for tag-based filtering
        query = "SELECT entity_id FROM search_entities_by_tags($1, $2, $3)"
        
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch(
                query,
                filter_request.entity_type,
                filter_request.tag_ids,
                filter_request.match_mode
            )
            return [row['entity_id'] for row in rows]
    
    async def get_tagged_issues(self, 
                               tag_ids: Optional[List[int]] = None,
                               symbols: Optional[List[str]] = None,
                               date_from: Optional[datetime] = None,
                               date_to: Optional[datetime] = None,
                               limit: int = 50,
                               offset: int = 0) -> List[TaggedIssue]:
        """Get data quality issues with their tags, filtered by various criteria"""
        conditions = ["i.id IS NOT NULL"]
        params = []
        param_count = 0
        
        # Build WHERE conditions
        if tag_ids:
            # Get entity IDs that have any of the specified tags
            tag_filter = TagFilter(
                entity_type="data_quality_issues",
                tag_ids=tag_ids,
                match_mode="ANY"
            )
            entity_ids = await self.filter_entities_by_tags(tag_filter)
            if entity_ids:
                param_count += 1
                conditions.append(f"i.id = ANY(${param_count})")
                params.append(entity_ids)
            else:
                # No entities match the tag filter
                return []
        
        if symbols:
            param_count += 1
            conditions.append(f"i.symbol = ANY(${param_count})")
            params.append(symbols)
        
        if date_from:
            param_count += 1
            conditions.append(f"i.affected_date >= ${param_count}")
            params.append(date_from.date())
            
        if date_to:
            param_count += 1
            conditions.append(f"i.affected_date <= ${param_count}")
            params.append(date_to.date())
        
        where_clause = " AND ".join(conditions)
        
        # Query to get issues with tag aggregation
        query = f"""
        WITH issue_tags AS (
            SELECT 
                et.entity_id as issue_id,
                array_agg(
                    json_build_object(
                        'id', t.id,
                        'name', t.name,
                        'slug', t.slug,
                        'color', t.color,
                        'category', tc.name
                    ) ORDER BY tc.sort_order, t.name
                ) as tags,
                count(et.id) as tag_count
            FROM entity_tags et
            JOIN tags t ON et.tag_id = t.id
            LEFT JOIN tag_categories tc ON t.category_id = tc.id
            JOIN entity_types ety ON et.entity_type_id = ety.id
            WHERE ety.name = 'data_quality_issues'
            GROUP BY et.entity_id
        )
        SELECT 
            i.id, i.symbol, i.issue_type, i.description, i.severity,
            i.affected_date, i.vendor_source, i.field, i.expected_value,
            i.actual_value, i.created_at, i.updated_at,
            COALESCE(it.tags, '[]'::json[]) as tags,
            COALESCE(it.tag_count, 0) as tag_count
        FROM dev_data_quality_issues i
        LEFT JOIN issue_tags it ON i.id = it.issue_id
        WHERE {where_clause}
        ORDER BY i.created_at DESC
        LIMIT ${param_count + 1} OFFSET ${param_count + 2}
        """
        
        params.extend([limit, offset])
        
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
            issues = []
            
            for row in rows:
                # Parse tags JSON
                tags = []
                if row['tags']:
                    for tag_json in row['tags']:
                        if isinstance(tag_json, str):
                            import json
                            tag_data = json.loads(tag_json)
                        else:
                            tag_data = tag_json
                        
                        tag = Tag(
                            id=tag_data['id'],
                            name=tag_data['name'],
                            slug=tag_data['slug'],
                            description=None,
                            color=tag_data['color'],
                            category_id=None,
                            category=None,
                            created_at=datetime.now(),
                            updated_at=datetime.now(),
                            usage_count=0,
                            is_system_tag=False,
                            is_active=True,
                            metadata={}
                        )
                        tags.append(tag)
                
                issue = TaggedIssue(
                    id=row['id'],
                    symbol=row['symbol'],
                    issue_type=row['issue_type'],
                    description=row['description'],
                    severity=row['severity'],
                    affected_date=row['affected_date'],
                    vendor_source=row['vendor_source'],
                    field=row['field'],
                    expected_value=row['expected_value'],
                    actual_value=row['actual_value'],
                    created_at=row['created_at'],
                    updated_at=row['updated_at'],
                    tags=tags,
                    tag_count=row['tag_count']
                )
                issues.append(issue)
            
            return issues
    
    async def get_tag_usage_stats(self, days: int = 30) -> List[TagUsageStats]:
        """Get tag usage statistics"""
        query = """
        SELECT id, name, slug, color, category_id, category_name,
               total_usage, unique_entities, entity_types_count,
               avg_confidence, last_used, active_days_last_90
        FROM tag_usage_summary
        WHERE total_usage > 0
        ORDER BY total_usage DESC
        LIMIT 50
        """
        
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch(query)
            return [self._row_to_tag_usage_stats(row) for row in rows]
    
    async def bulk_apply_tags(self, request: BulkTagRequest, user_id: Optional[str] = None) -> List[int]:
        """Apply multiple tags to multiple entities"""
        entity_type_id = await self._get_entity_type_id(request.entity_type)
        if not entity_type_id:
            raise ValueError(f"Unknown entity type: {request.entity_type}")
        
        # Prepare batch insert data
        insert_data = []
        for entity_id in request.entity_ids:
            for tag_id in request.tag_ids:
                insert_data.append((
                    entity_type_id,
                    entity_id,
                    tag_id,
                    user_id,
                    request.confidence_score,
                    request.source.value,
                    request.metadata or {}
                ))
        
        query = """
        INSERT INTO entity_tags (entity_type_id, entity_id, tag_id, tagged_by_user_id, 
                               confidence_score, source, metadata)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (entity_type_id, entity_id, tag_id) 
        DO UPDATE SET
            confidence_score = EXCLUDED.confidence_score,
            source = EXCLUDED.source,
            metadata = EXCLUDED.metadata,
            tagged_at = CURRENT_TIMESTAMP
        RETURNING id
        """
        
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch_many(query, insert_data)
            return [row['id'] for row in rows]
    
    async def refresh_tag_stats(self):
        """Refresh the materialized view for tag statistics"""
        async with self.db_pool.acquire() as conn:
            await conn.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY tag_usage_summary")
    
    # Helper methods
    async def _get_entity_type_id(self, entity_type_name: str) -> Optional[int]:
        """Get entity type ID by name"""
        query = "SELECT id FROM entity_types WHERE name = $1"
        async with self.db_pool.acquire() as conn:
            row = await conn.fetchrow(query, entity_type_name)
            return row['id'] if row else None
    
    def _generate_slug(self, name: str) -> str:
        """Generate URL-friendly slug from tag name"""
        import re
        slug = re.sub(r'[^\w\s-]', '', name.lower())
        slug = re.sub(r'[-\s]+', '-', slug)
        return slug.strip('-')
    
    def _row_to_tag(self, row) -> Tag:
        """Convert database row to Tag object"""
        category = None
        if row.get('category_name'):
            category = TagCategory(
                id=row['category_id'],
                name=row['category_name'],
                slug=row.get('category_slug', ''),
                description=None,
                color=row.get('category_color', '#6c757d'),
                icon=row.get('category_icon', 'tag'),
                parent_id=None,
                sort_order=0,
                created_at=datetime.now(),
                updated_at=datetime.now()
            )
        
        return Tag(
            id=row['id'],
            name=row['name'],
            slug=row['slug'],
            description=row.get('description'),
            color=row['color'],
            category_id=row.get('category_id'),
            category=category,
            created_at=row['created_at'],
            updated_at=row['updated_at'],
            usage_count=row['usage_count'],
            is_system_tag=row['is_system_tag'],
            is_active=row['is_active'],
            metadata=row.get('metadata', {})
        )
    
    def _row_to_tag_category(self, row) -> TagCategory:
        """Convert database row to TagCategory object"""
        return TagCategory(
            id=row['id'],
            name=row['name'],
            slug=row['slug'],
            description=row.get('description'),
            color=row['color'],
            icon=row['icon'],
            parent_id=row.get('parent_id'),
            sort_order=row['sort_order'],
            created_at=row['created_at'],
            updated_at=row['updated_at']
        )
    
    def _row_to_entity_type(self, row) -> EntityType:
        """Convert database row to EntityType object"""
        return EntityType(
            id=row['id'],
            name=row['name'],
            table_name=row['table_name'],
            display_name=row['display_name'],
            description=row.get('description'),
            is_active=row['is_active'],
            created_at=row['created_at']
        )
    
    def _row_to_tag_usage_stats(self, row) -> TagUsageStats:
        """Convert database row to TagUsageStats object"""
        return TagUsageStats(
            tag_id=row['id'],
            tag_name=row['name'],
            total_usage=row['total_usage'],
            unique_entities=row['unique_entities'],
            entity_types_count=row['entity_types_count'],
            avg_confidence=float(row['avg_confidence']) if row['avg_confidence'] else 0.0,
            last_used=row.get('last_used'),
            active_days_last_90=row['active_days_last_90']
        )