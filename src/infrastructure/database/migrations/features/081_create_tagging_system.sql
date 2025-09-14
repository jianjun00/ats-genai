-- Migration: Create comprehensive tagging system for ATS platform
-- Version: 081
-- Description: Generic tagging system for issues, datasets, models with filtering and analytics

-- Tag categories for organization
CREATE TABLE IF NOT EXISTS tag_categories (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL,
    slug VARCHAR(100) UNIQUE NOT NULL,
    description TEXT,
    color VARCHAR(7) DEFAULT '#3498db', -- hex color for UI
    icon VARCHAR(50) DEFAULT 'tag',
    parent_id INTEGER REFERENCES tag_categories(id),
    sort_order INTEGER DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Core tags table
CREATE TABLE IF NOT EXISTS tags (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    slug VARCHAR(255) UNIQUE NOT NULL, -- URL-friendly version
    description TEXT,
    color VARCHAR(7) DEFAULT '#6c757d', -- hex color code for UI
    category_id INTEGER REFERENCES tag_categories(id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    usage_count INTEGER DEFAULT 0, -- denormalized for performance
    is_system_tag BOOLEAN DEFAULT FALSE, -- auto-generated vs user-created
    is_active BOOLEAN DEFAULT TRUE,
    metadata JSONB DEFAULT '{}', -- flexible attributes
    UNIQUE(name, category_id) -- allow same name in different categories
);

-- Entity types registry
CREATE TABLE IF NOT EXISTS entity_types (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL, -- 'data_quality_issues', 'datasets', 'models'
    table_name VARCHAR(100) NOT NULL,  -- actual table name for joins
    display_name VARCHAR(100) NOT NULL,
    description TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Generic entity-tag junction table
CREATE TABLE IF NOT EXISTS entity_tags (
    id BIGSERIAL PRIMARY KEY,
    entity_type_id INTEGER NOT NULL REFERENCES entity_types(id),
    entity_id BIGINT NOT NULL, -- generic foreign key (can reference any table)
    tag_id BIGINT NOT NULL REFERENCES tags(id),
    tagged_by_user_id VARCHAR(100), -- who applied the tag
    tagged_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    confidence_score DECIMAL(3,2) DEFAULT 1.00, -- for auto-tagging confidence (0.00-1.00)
    source VARCHAR(50) DEFAULT 'manual', -- 'manual', 'auto', 'rule', 'ml'
    metadata JSONB DEFAULT '{}', -- flexible tagging context
    UNIQUE(entity_type_id, entity_id, tag_id)
);

-- Tag usage analytics (for trending and insights)
CREATE TABLE IF NOT EXISTS tag_usage_metrics (
    date DATE NOT NULL,
    tag_id BIGINT NOT NULL REFERENCES tags(id),
    entity_type_id INTEGER NOT NULL REFERENCES entity_types(id),
    new_usages INTEGER DEFAULT 0,
    total_usages INTEGER DEFAULT 0,
    unique_entities INTEGER DEFAULT 0,
    avg_confidence_score DECIMAL(3,2),
    PRIMARY KEY (date, tag_id, entity_type_id)
);

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_tags_name ON tags(name);
CREATE INDEX IF NOT EXISTS idx_tags_slug ON tags(slug);
CREATE INDEX IF NOT EXISTS idx_tags_category ON tags(category_id);
CREATE INDEX IF NOT EXISTS idx_tags_usage_count ON tags(usage_count DESC);
CREATE INDEX IF NOT EXISTS idx_tags_active ON tags(is_active) WHERE is_active = true;

-- Composite indexes for entity_tags (critical for performance)
CREATE INDEX IF NOT EXISTS idx_entity_tags_entity_lookup ON entity_tags(entity_type_id, entity_id);
CREATE INDEX IF NOT EXISTS idx_entity_tags_tag_lookup ON entity_tags(tag_id);
CREATE INDEX IF NOT EXISTS idx_entity_tags_composite ON entity_tags(entity_type_id, entity_id, tag_id);
CREATE INDEX IF NOT EXISTS idx_entity_tags_tagged_at ON entity_tags(tagged_at DESC);
CREATE INDEX IF NOT EXISTS idx_entity_tags_source ON entity_tags(source);

-- Partial indexes for performance optimization
CREATE INDEX IF NOT EXISTS idx_system_tags ON tags(name) WHERE is_system_tag = true;
CREATE INDEX IF NOT EXISTS idx_user_tags ON tags(name) WHERE is_system_tag = false;
CREATE INDEX IF NOT EXISTS idx_manual_tags ON entity_tags(entity_type_id, entity_id) WHERE source = 'manual';

-- GIN index for JSONB metadata searches
CREATE INDEX IF NOT EXISTS idx_tags_metadata ON tags USING gin(metadata);
CREATE INDEX IF NOT EXISTS idx_entity_tags_metadata ON entity_tags USING gin(metadata);

-- Full-text search index for tag names and descriptions
CREATE INDEX IF NOT EXISTS idx_tags_search ON tags USING gin(to_tsvector('english', name || ' ' || COALESCE(description, '')));

-- Insert default tag categories
INSERT INTO tag_categories (name, slug, description, color, icon, sort_order) VALUES
('Priority', 'priority', 'Issue priority levels', '#e74c3c', 'exclamation-triangle', 1),
('Status', 'status', 'Workflow and resolution status', '#3498db', 'check-circle', 2),
('Source', 'source', 'Data source identification', '#9b59b6', 'database', 3),
('Type', 'type', 'Issue classification types', '#f39c12', 'tags', 4),
('Impact', 'impact', 'Business impact levels', '#e67e22', 'bolt', 5),
('Quality', 'quality', 'Data quality aspects', '#27ae60', 'shield-alt', 6),
('Custom', 'custom', 'User-defined tags', '#95a5a6', 'user', 100)
ON CONFLICT (name) DO NOTHING;

-- Insert default entity types
INSERT INTO entity_types (name, table_name, display_name, description) VALUES
('data_quality_issues', 'dev_data_quality_issues', 'Data Quality Issues', 'Issues detected by data quality monitoring'),
('datasets', 'dev_training_dataset', 'Training Datasets', 'ML training datasets'),
('models', 'dev_models', 'ML Models', 'Machine learning models'),
('runs', 'dev_runs', 'Execution Runs', 'Training and processing runs')
ON CONFLICT (name) DO NOTHING;

-- Insert system tags for data quality issues
WITH category_ids AS (
    SELECT id as priority_id FROM tag_categories WHERE slug = 'priority'
    UNION ALL
    SELECT id as status_id FROM tag_categories WHERE slug = 'status'  
    UNION ALL
    SELECT id as source_id FROM tag_categories WHERE slug = 'source'
    UNION ALL
    SELECT id as type_id FROM tag_categories WHERE slug = 'type'
    UNION ALL
    SELECT id as impact_id FROM tag_categories WHERE slug = 'impact'
    UNION ALL
    SELECT id as quality_id FROM tag_categories WHERE slug = 'quality'
)
INSERT INTO tags (name, slug, description, color, category_id, is_system_tag) 
SELECT * FROM (VALUES
    -- Priority tags
    ('Critical', 'critical', 'Critical priority requiring immediate attention', '#e74c3c', (SELECT id FROM tag_categories WHERE slug = 'priority'), true),
    ('High', 'high', 'High priority issues', '#ff6b6b', (SELECT id FROM tag_categories WHERE slug = 'priority'), true),
    ('Medium', 'medium', 'Medium priority issues', '#ffa726', (SELECT id FROM tag_categories WHERE slug = 'priority'), true),
    ('Low', 'low', 'Low priority issues', '#66bb6a', (SELECT id FROM tag_categories WHERE slug = 'priority'), true),
    
    -- Status tags
    ('Open', 'open', 'Open and unresolved', '#3498db', (SELECT id FROM tag_categories WHERE slug = 'status'), true),
    ('In Progress', 'in-progress', 'Currently being investigated', '#f39c12', (SELECT id FROM tag_categories WHERE slug = 'status'), true),
    ('Resolved', 'resolved', 'Issue has been resolved', '#27ae60', (SELECT id FROM tag_categories WHERE slug = 'status'), true),
    ('False Positive', 'false-positive', 'Not a real issue', '#95a5a6', (SELECT id FROM tag_categories WHERE slug = 'status'), true),
    
    -- Source tags  
    ('Polygon', 'polygon', 'Issues from Polygon data source', '#8e44ad', (SELECT id FROM tag_categories WHERE slug = 'source'), true),
    ('Tiingo', 'tiingo', 'Issues from Tiingo data source', '#2ecc71', (SELECT id FROM tag_categories WHERE slug = 'source'), true),
    ('EODHD', 'eodhd', 'Issues from EODHD data source', '#e67e22', (SELECT id FROM tag_categories WHERE slug = 'source'), true),
    ('FirstRate', 'firstrate', 'Issues from FirstRate data source', '#34495e', (SELECT id FROM tag_categories WHERE slug = 'source'), true),
    
    -- Type tags
    ('Data Gap', 'data-gap', 'Missing data periods', '#e74c3c', (SELECT id FROM tag_categories WHERE slug = 'type'), true),
    ('Price Anomaly', 'price-anomaly', 'Unusual price movements or values', '#f39c12', (SELECT id FROM tag_categories WHERE slug = 'type'), true),
    ('Volume Spike', 'volume-spike', 'Unusual trading volume', '#9b59b6', (SELECT id FROM tag_categories WHERE slug = 'type'), true),
    ('Data Quality', 'data-quality', 'General data quality issues', '#3498db', (SELECT id FROM tag_categories WHERE slug = 'type'), true),
    ('Duplicate Data', 'duplicate-data', 'Duplicate or redundant data', '#95a5a6', (SELECT id FROM tag_categories WHERE slug = 'type'), true),
    
    -- Impact tags
    ('Trading Halt', 'trading-halt', 'Could impact trading operations', '#e74c3c', (SELECT id FROM tag_categories WHERE slug = 'impact'), true),
    ('System Wide', 'system-wide', 'Affects multiple systems or symbols', '#ff6b6b', (SELECT id FROM tag_categories WHERE slug = 'impact'), true),
    ('Minor', 'minor', 'Minimal business impact', '#66bb6a', (SELECT id FROM tag_categories WHERE slug = 'impact'), true),
    
    -- Quality tags
    ('Accuracy', 'accuracy', 'Data accuracy concerns', '#3498db', (SELECT id FROM tag_categories WHERE slug = 'quality'), true),
    ('Completeness', 'completeness', 'Data completeness issues', '#2ecc71', (SELECT id FROM tag_categories WHERE slug = 'quality'), true),
    ('Timeliness', 'timeliness', 'Data freshness and timing issues', '#f39c12', (SELECT id FROM tag_categories WHERE slug = 'quality'), true),
    ('Consistency', 'consistency', 'Data consistency problems', '#9b59b6', (SELECT id FROM tag_categories WHERE slug = 'quality'), true)
) AS v(name, slug, description, color, category_id, is_system_tag)
ON CONFLICT (slug) DO NOTHING;

-- Update usage counts for initial tags
UPDATE tags SET usage_count = 0 WHERE usage_count IS NULL;

-- Trigger to update tag usage_count when entity_tags change
CREATE OR REPLACE FUNCTION update_tag_usage_count()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        UPDATE tags SET usage_count = usage_count + 1 WHERE id = NEW.tag_id;
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        UPDATE tags SET usage_count = usage_count - 1 WHERE id = OLD.tag_id;
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_tag_usage_count ON entity_tags;
CREATE TRIGGER trigger_update_tag_usage_count
    AFTER INSERT OR DELETE ON entity_tags
    FOR EACH ROW EXECUTE FUNCTION update_tag_usage_count();

-- Function to get tags for an entity
CREATE OR REPLACE FUNCTION get_entity_tags(p_entity_type VARCHAR, p_entity_id BIGINT)
RETURNS TABLE (
    tag_id BIGINT,
    tag_name VARCHAR,
    tag_slug VARCHAR,
    tag_color VARCHAR,
    category_name VARCHAR,
    tagged_at TIMESTAMP WITH TIME ZONE,
    confidence_score DECIMAL,
    source VARCHAR
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        t.id,
        t.name,
        t.slug,
        t.color,
        tc.name as category_name,
        et.tagged_at,
        et.confidence_score,
        et.source
    FROM entity_tags et
    JOIN tags t ON et.tag_id = t.id
    LEFT JOIN tag_categories tc ON t.category_id = tc.id
    JOIN entity_types ety ON et.entity_type_id = ety.id
    WHERE ety.name = p_entity_type
    AND et.entity_id = p_entity_id
    AND t.is_active = true
    ORDER BY tc.sort_order, t.name;
END;
$$ LANGUAGE plpgsql;

-- Function to search entities by tags
CREATE OR REPLACE FUNCTION search_entities_by_tags(
    p_entity_type VARCHAR,
    p_tag_ids BIGINT[],
    p_match_mode VARCHAR DEFAULT 'ANY' -- 'ANY' or 'ALL'
)
RETURNS TABLE (entity_id BIGINT) AS $$
DECLARE
    entity_type_id_val INTEGER;
BEGIN
    -- Get entity type ID
    SELECT id INTO entity_type_id_val FROM entity_types WHERE name = p_entity_type;
    
    IF entity_type_id_val IS NULL THEN
        RAISE EXCEPTION 'Unknown entity type: %', p_entity_type;
    END IF;
    
    IF p_match_mode = 'ALL' THEN
        -- Return entities that have ALL specified tags
        RETURN QUERY
        SELECT et.entity_id
        FROM entity_tags et
        WHERE et.entity_type_id = entity_type_id_val
        AND et.tag_id = ANY(p_tag_ids)
        GROUP BY et.entity_id
        HAVING COUNT(DISTINCT et.tag_id) = array_length(p_tag_ids, 1);
    ELSE
        -- Return entities that have ANY of the specified tags
        RETURN QUERY
        SELECT DISTINCT et.entity_id
        FROM entity_tags et
        WHERE et.entity_type_id = entity_type_id_val
        AND et.tag_id = ANY(p_tag_ids);
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Create materialized view for tag statistics (refresh periodically)
CREATE MATERIALIZED VIEW IF NOT EXISTS tag_usage_summary AS
SELECT 
    t.id,
    t.name,
    t.slug,
    t.color,
    t.category_id,
    tc.name as category_name,
    COUNT(et.id) as total_usage,
    COUNT(DISTINCT et.entity_id) as unique_entities,
    COUNT(DISTINCT et.entity_type_id) as entity_types_count,
    AVG(et.confidence_score) as avg_confidence,
    MAX(et.tagged_at) as last_used,
    COUNT(DISTINCT DATE(et.tagged_at)) as active_days_last_90
FROM tags t
LEFT JOIN entity_tags et ON t.id = et.tag_id AND et.tagged_at >= CURRENT_DATE - INTERVAL '90 days'
LEFT JOIN tag_categories tc ON t.category_id = tc.id
WHERE t.is_active = true
GROUP BY t.id, t.name, t.slug, t.color, t.category_id, tc.name;

-- Create unique index for concurrent refresh
CREATE UNIQUE INDEX IF NOT EXISTS idx_tag_usage_summary_id ON tag_usage_summary (id);

-- Comment the tables
COMMENT ON TABLE tags IS 'Core tags that can be applied to any entity type';
COMMENT ON TABLE tag_categories IS 'Categories for organizing tags hierarchically';
COMMENT ON TABLE entity_types IS 'Registry of entity types that support tagging';
COMMENT ON TABLE entity_tags IS 'Junction table linking entities to tags with metadata';
COMMENT ON TABLE tag_usage_metrics IS 'Analytics data for tag usage trends and insights';

-- Additional indexes for tag_usage_metrics
CREATE INDEX IF NOT EXISTS idx_tag_usage_metrics_date ON tag_usage_metrics(date);
CREATE INDEX IF NOT EXISTS idx_tag_usage_metrics_tag ON tag_usage_metrics(tag_id);
COMMENT ON MATERIALIZED VIEW tag_usage_summary IS 'Aggregated tag usage statistics for dashboard display';