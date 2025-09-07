from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from enum import Enum


class APIKeyTier(str, Enum):
    FREE = "free"
    PREMIUM = "premium"


class APIKeyStatus(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    REVOKED = "revoked"


@dataclass
class APIKey:
    """API Key model for authentication system"""
    id: int
    key_hash: str
    key_prefix: str
    tier: APIKeyTier
    status: APIKeyStatus
    name: Optional[str] = None
    description: Optional[str] = None
    created_at: Optional[datetime] = None
    last_used_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    created_by: Optional[str] = None


@dataclass
class APIUsage:
    """API Usage tracking model"""
    id: int
    api_key_id: int
    endpoint: str
    method: str
    status_code: int
    request_count: int
    date: datetime
    hour: int
    response_time_ms: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class AuthContext:
    """Authentication context injected into requests"""
    api_key_id: int
    tier: APIKeyTier
    key_prefix: str
    daily_usage: int
    rate_limit_remaining: int
    is_premium: bool

    @property
    def rate_limit(self) -> int:
        """Get rate limit based on tier"""
        return float('inf') if self.is_premium else 24

    @property
    def within_rate_limit(self) -> bool:
        """Check if within rate limit"""
        return self.is_premium or self.daily_usage < 24