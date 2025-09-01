from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any
from enum import Enum


class SubscriptionTier(str, Enum):
    FREE = "free"
    PREMIUM = "premium"


class SubscriptionStatus(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    CANCELLED = "cancelled"


@dataclass
class User:
    """User model for Google OAuth2 authenticated users"""
    id: int
    google_id: str
    email: str
    name: Optional[str] = None
    given_name: Optional[str] = None
    family_name: Optional[str] = None
    picture_url: Optional[str] = None
    subscription_tier: SubscriptionTier = SubscriptionTier.FREE
    subscription_status: SubscriptionStatus = SubscriptionStatus.ACTIVE
    stripe_customer_id: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    last_login_at: Optional[datetime] = None
    email_verified: bool = False
    is_active: bool = True
    
    @property
    def is_premium(self) -> bool:
        return self.subscription_tier == SubscriptionTier.PREMIUM
    
    @property
    def display_name(self) -> str:
        return self.name or self.email or "User"
    
    @property
    def rate_limit(self) -> int:
        return float('inf') if self.is_premium else 24


@dataclass
class UserSession:
    """User session for dashboard authentication"""
    id: int
    user_id: int
    session_token: str
    refresh_token: Optional[str] = None
    expires_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    last_used_at: Optional[datetime] = None
    user_agent: Optional[str] = None
    ip_address: Optional[str] = None
    is_active: bool = True


@dataclass
class UserAPIKey:
    """User-generated API key model"""
    id: int
    user_id: int
    key_hash: str
    key_prefix: str
    name: str
    description: Optional[str] = None
    permissions: Dict[str, Any] = None
    status: str = "active"
    created_at: Optional[datetime] = None
    last_used_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    usage_count: int = 0
    
    def __post_init__(self):
        if self.permissions is None:
            self.permissions = {"recommendations": True, "usage": True}


@dataclass
class UserPreferences:
    """User preferences and settings"""
    id: int
    user_id: int
    watchlist: list = None
    notification_settings: Dict[str, Any] = None
    dashboard_layout: Dict[str, Any] = None
    timezone: str = "UTC"
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    def __post_init__(self):
        if self.watchlist is None:
            self.watchlist = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]
        if self.notification_settings is None:
            self.notification_settings = {"email": True, "push": False}
        if self.dashboard_layout is None:
            self.dashboard_layout = {"view": "grid", "refresh_interval": 3600}


@dataclass
class GoogleUserInfo:
    """Google OAuth2 user information"""
    id: str  # Google ID
    email: str
    verified_email: bool
    name: str
    given_name: str
    family_name: str
    picture: str
    locale: str
    
    @classmethod
    def from_google_response(cls, data: Dict[str, Any]) -> 'GoogleUserInfo':
        """Create from Google OAuth2 userinfo response"""
        return cls(
            id=data.get('id', ''),
            email=data.get('email', ''),
            verified_email=data.get('verified_email', False),
            name=data.get('name', ''),
            given_name=data.get('given_name', ''),
            family_name=data.get('family_name', ''),
            picture=data.get('picture', ''),
            locale=data.get('locale', 'en')
        )


@dataclass
class AuthContext:
    """Authentication context for API requests"""
    user: User
    session: Optional[UserSession] = None
    api_key: Optional[UserAPIKey] = None
    daily_usage: int = 0
    rate_limit_remaining: int = 0
    
    @property
    def is_premium(self) -> bool:
        return self.user.is_premium
    
    @property
    def within_rate_limit(self) -> bool:
        return self.is_premium or self.daily_usage < 24
    
    @property
    def user_id(self) -> int:
        return self.user.id