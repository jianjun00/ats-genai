"""
Data models for the tagging system
"""
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum


class TagSource(Enum):
    MANUAL = "manual"
    AUTO = "auto"
    RULE = "rule"
    ML = "ml"


@dataclass
class TagCategory:
    id: int
    name: str
    slug: str
    description: Optional[str]
    color: str
    icon: str
    parent_id: Optional[int]
    sort_order: int
    created_at: datetime
    updated_at: datetime


@dataclass
class Tag:
    id: int
    name: str
    slug: str
    description: Optional[str]
    color: str
    category_id: Optional[int]
    category: Optional[TagCategory]
    created_at: datetime
    updated_at: datetime
    usage_count: int
    is_system_tag: bool
    is_active: bool
    metadata: Dict[str, Any]


@dataclass
class EntityType:
    id: int
    name: str
    table_name: str
    display_name: str
    description: Optional[str]
    is_active: bool
    created_at: datetime


@dataclass
class EntityTag:
    id: int
    entity_type_id: int
    entity_id: int
    tag_id: int
    tagged_by_user_id: Optional[str]
    tagged_at: datetime
    confidence_score: float
    source: TagSource
    metadata: Dict[str, Any]


@dataclass
class TaggedEntity:
    """Entity with its associated tags"""
    entity_type: str
    entity_id: int
    tags: List[Tag]
    total_tags: int


@dataclass
class TagUsageStats:
    """Tag usage statistics"""
    tag_id: int
    tag_name: str
    total_usage: int
    unique_entities: int
    entity_types_count: int
    avg_confidence: float
    last_used: Optional[datetime]
    active_days_last_90: int


@dataclass
class TagFilter:
    """Filter parameters for tag-based searches"""
    entity_type: str
    tag_ids: Optional[List[int]] = None
    categories: Optional[List[int]] = None
    symbols: Optional[List[str]] = None
    date_from: Optional[datetime] = None
    date_to: Optional[datetime] = None
    search: Optional[str] = None
    match_mode: str = "ANY"  # "ANY" or "ALL" for tag matching
    limit: int = 50
    offset: int = 0


@dataclass
class TagSuggestion:
    """Auto-tagging suggestion"""
    tag_id: int
    tag_name: str
    confidence_score: float
    source: TagSource
    explanation: Optional[str] = None


@dataclass
class TagAnalytics:
    """Tag analytics data for dashboard"""
    most_used_tags: List[TagUsageStats]
    tag_categories_distribution: Dict[str, int]
    tagging_trends: Dict[str, int]  # date -> count
    entity_coverage: float  # percentage of entities with tags
    avg_tags_per_entity: float
    top_co_occurring_tags: List[Dict[str, Any]]


@dataclass
class CreateTagRequest:
    name: str
    description: Optional[str] = None
    category_id: Optional[int] = None
    color: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class ApplyTagRequest:
    entity_type: str
    entity_id: int
    tag_id: int
    confidence_score: float = 1.0
    source: TagSource = TagSource.MANUAL
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class BulkTagRequest:
    """Request to apply multiple tags to multiple entities"""
    entity_type: str
    entity_ids: List[int]
    tag_ids: List[int]
    confidence_score: float = 1.0
    source: TagSource = TagSource.MANUAL
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class TaggedIssue:
    """Data quality issue with tags"""
    id: int
    symbol: str
    issue_type: str
    description: str
    severity: str
    affected_date: str
    vendor_source: str
    field: str
    expected_value: Optional[str]
    actual_value: Optional[str]
    created_at: datetime
    updated_at: datetime
    tags: List[Tag]
    tag_count: int